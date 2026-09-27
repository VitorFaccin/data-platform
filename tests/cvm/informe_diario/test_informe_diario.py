"""cvm_informe_diario: which months a run checks, how a file is parsed, and re-run safety.

Three tiers in one file, cheapest first:
- **window** and **change detection** — pure date and fingerprint rules;
- **contract** — the parser against hand-typed fixtures in both layouts, with every
  rejection asserted as eagerly as the successes;
- **idempotency** — the real write path against temporary Delta tables, run twice.
"""

from __future__ import annotations

import datetime as dt
import io
import zipfile
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest

from cvm.informe_diario import adapters
from cvm.informe_diario.core import domain, schema

FIXTURES = Path(__file__).parent / "fixtures"
NOV_2023 = dt.date(2023, 11, 1)
SEP_2026 = dt.date(2026, 9, 1)
SHA = "f" * 64


def _zip(month: dt.date, csv: bytes, member: str | None = None) -> bytes:
    """Pack a CSV into a zip the way CVM publishes it.

    Args:
        month (dt.date): Month that names the member file.
        csv (bytes): CSV content.
        member (str | None): Override the member name (to test a wrong one).

    Returns:
        bytes: The zip file content.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(member or f"inf_diario_fi_{month:%Y%m}.csv", csv)
    return buffer.getvalue()


def _fixture(name: str) -> bytes:
    """Read a fixture byte-exact (CRLF and encoding preserved).

    Args:
        name (str): File name under fixtures/.

    Returns:
        bytes: File content.
    """
    return (FIXTURES / name).read_bytes()


def _v1() -> bytes:
    """Return the layout v1 sample (until 2023-11).

    Returns:
        bytes: CSV content.
    """
    return _fixture("inf_diario_v1_sample.csv")


def _v2() -> bytes:
    """Return the layout v2 sample (from 2023-12, CVM 175).

    Returns:
        bytes: CSV content.
    """
    return _fixture("inf_diario_v2_sample.csv")


def _replace_line(csv: bytes, index: int, line: str) -> bytes:
    """Swap one CSV line, keeping CRLF line ends.

    Args:
        csv (bytes): Original CSV content.
        index (int): Line to replace; 0 is the header.
        line (str): Replacement line, ASCII.

    Returns:
        bytes: The edited CSV content.
    """
    lines = csv.split(b"\r\n")
    lines[index] = line.encode("ascii")
    return b"\r\n".join(lines)


def _remote(etag: str = '"a-1"') -> schema.RemoteFile:
    """Build source metadata for the September 2026 file.

    Args:
        etag (str): ETag the source reports.

    Returns:
        schema.RemoteFile: Metadata with fixed modification time and size.
    """
    return schema.RemoteFile(
        url=domain.source_url(SEP_2026),
        etag=etag,
        last_modified=dt.datetime(2026, 9, 26, 3, 50, tzinfo=dt.UTC),
        size_bytes=123,
    )


def test_weekday_checks_current_and_previous_month() -> None:
    wednesday = dt.date(2026, 9, 23)
    assert domain.months_to_check(wednesday) == [dt.date(2026, 8, 1), SEP_2026]


def test_sunday_checks_the_full_13_month_window() -> None:
    sunday = dt.date(2026, 9, 27)
    months = domain.months_to_check(sunday)
    assert len(months) == 13
    assert months[0] == dt.date(2025, 9, 1)
    assert months[-1] == SEP_2026


def test_full_window_param_forces_13_months_on_a_weekday() -> None:
    assert len(domain.months_to_check(dt.date(2026, 9, 23), full_window=True)) == 13


def test_window_crosses_the_year_boundary() -> None:
    assert domain.months_to_check(dt.date(2026, 1, 14)) == [
        dt.date(2025, 12, 1),
        dt.date(2026, 1, 1),
    ]


def test_window_never_reaches_before_the_first_monthly_file() -> None:
    sunday_in_2021 = dt.date(2021, 6, 6)
    assert domain.months_to_check(sunday_in_2021)[0] == domain.FIRST_MONTHLY_FILE


def test_requested_months_are_validated_sorted_and_deduplicated() -> None:
    months = domain.validate_requested_months(["2026-09", "2021-01", "2026-09"], SEP_2026)
    assert months == [dt.date(2021, 1, 1), SEP_2026]


@pytest.mark.parametrize(
    ("value", "message"),
    [("2026-9", "YYYY-MM"), ("2026-13", "YYYY-MM"), ("2020-12", "HIST"), ("2026-10", "future")],
)
def test_requested_months_reject_bad_input(value: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        domain.validate_requested_months([value], dt.date(2026, 9, 27))


def test_only_the_current_month_may_be_missing() -> None:
    targets = domain.plan_targets([dt.date(2026, 8, 1), SEP_2026], dt.date(2026, 9, 1))
    assert targets == [
        {"month": "2026-08-01", "may_be_missing": False, "force": False},
        {"month": "2026-09-01", "may_be_missing": True, "force": False},
    ]


def test_force_reaches_every_month() -> None:
    targets = domain.plan_targets([dt.date(2025, 9, 1), dt.date(2025, 10, 1)], SEP_2026, True)
    assert all(target["force"] for target in targets)


def test_never_ingested_month_is_never_unchanged() -> None:
    assert not domain.etag_unchanged(_remote(), None)
    assert not domain.content_unchanged(SHA, None)


def test_same_etag_means_unchanged_without_downloading() -> None:
    current = schema.ManifestEntry(SEP_2026, sha256=SHA, etag='"a-1"')
    assert domain.etag_unchanged(_remote('"a-1"'), current)
    assert not domain.etag_unchanged(_remote('"b-2"'), current)


def test_a_return_to_an_older_version_rewrites_bronze() -> None:
    """A → B → A: bronze holds B, so arriving at A again is a change, not a repeat."""
    sha_a, sha_b = "a" * 64, "b" * 64
    in_bronze = schema.ManifestEntry(SEP_2026, sha256=sha_b, etag='"b"')
    assert not domain.content_unchanged(sha_a, in_bronze)
    assert domain.content_unchanged(sha_b, in_bronze)


def test_v1_file_lands_in_the_unified_bronze_schema() -> None:
    frame = domain.parse_informe(_zip(NOV_2023, _v1()), NOV_2023, SHA)
    assert frame.schema == pl.Schema(schema.BRONZE_SCHEMA)
    assert frame.height == 5
    assert frame.get_column("layout_version").unique().to_list() == [1]
    assert frame.get_column("id_subclasse").null_count() == 5
    assert frame.get_column("reference_month").unique().to_list() == [NOV_2023]
    assert frame.get_column("source_sha256").unique().to_list() == [SHA]


def test_values_are_exact_decimals_including_negatives() -> None:
    frame = domain.parse_informe(_zip(NOV_2023, _v1()), NOV_2023, SHA)
    first = frame.row(0, named=True)
    assert first["vl_total"] == Decimal("1000000.50")
    assert first["vl_quota"] == Decimal("1.234567890123")
    negative = frame.filter(pl.col("cnpj_fundo_classe") == "33.333.333/0001-33").row(0, named=True)
    assert negative["vl_patrim_liq"] == Decimal("-150.10")


def test_v2_file_keeps_subclasses_apart() -> None:
    frame = domain.parse_informe(_zip(SEP_2026, _v2()), SEP_2026, SHA)
    assert frame.get_column("layout_version").unique().to_list() == [2]
    same_day = frame.filter(
        (pl.col("cnpj_fundo_classe") == "11.111.111/0001-11")
        & (pl.col("dt_comptc") == dt.date(2026, 9, 1))
    )
    assert set(same_day.get_column("id_subclasse").to_list()) == {"SUB0000000001", None}


def test_unknown_header_raises() -> None:
    csv = _replace_line(_v2(), 0, "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;DT_COMPTC;VL_TOTAL")
    with pytest.raises(domain.UnexpectedLayoutError, match="unknown header"):
        domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)


def test_zip_with_an_extra_member_raises() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("inf_diario_fi_202609.csv", _v2())
        archive.writestr("leia-me.txt", b"extra")
    with pytest.raises(domain.UnexpectedLayoutError, match="expected exactly"):
        domain.parse_informe(buffer.getvalue(), SEP_2026, SHA)


def test_zip_for_another_month_raises() -> None:
    with pytest.raises(domain.UnexpectedLayoutError, match="expected exactly"):
        domain.parse_informe(
            _zip(SEP_2026, _v2(), member="inf_diario_fi_202608.csv"), SEP_2026, SHA
        )


def test_not_a_zip_raises() -> None:
    with pytest.raises(domain.UnexpectedLayoutError, match="not a zip"):
        domain.parse_informe(b"<html>maintenance</html>", SEP_2026, SHA)


def test_non_utf8_bytes_raise() -> None:
    csv = _v2().replace(b"CLASSE FIF/FAPI", "CLASSE FIF/FAPÍ".encode("latin-1"))
    with pytest.raises(domain.UnexpectedLayoutError, match="UTF-8"):
        domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)


def test_header_without_rows_raises() -> None:
    header_only = _v2().split(b"\r\n")[0] + b"\r\n"
    with pytest.raises(domain.UnexpectedLayoutError, match="no rows"):
        domain.parse_informe(_zip(SEP_2026, header_only), SEP_2026, SHA)


def test_more_precision_than_published_raises_instead_of_rounding() -> None:
    """Polars would round 1.239 to 1.24 on the cast; the format check must stop it first."""
    csv = _replace_line(_v2(), 4, "FI;44.444.444/0001-44;;2026-09-25;42.239;1.0;42.00;0.00;0.00;1")
    with pytest.raises(domain.UnexpectedLayoutError, match="VL_TOTAL"):
        domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)


def test_empty_required_value_raises() -> None:
    csv = _replace_line(_v2(), 4, "FI;44.444.444/0001-44;;2026-09-25;42.00;1.0;42.00;0.00;0.00;")
    with pytest.raises(domain.UnexpectedLayoutError, match="empty values in NR_COTST"):
        domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)


def test_row_dated_outside_the_month_raises() -> None:
    csv = _replace_line(_v2(), 4, "FI;44.444.444/0001-44;;2026-10-01;42.00;1.0;42.00;0.00;0.00;1")
    with pytest.raises(domain.UnexpectedLayoutError, match="outside the month"):
        domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)


def test_source_duplicates_are_kept_and_counted() -> None:
    """The source repeats rows routinely (7 of the first 15 months). Bronze keeps them."""
    csv = _replace_line(
        _v2(), 5, "FI;11.111.111/0001-11;;2026-09-01;2000000.00;2.5;1999000.00;0.00;0.00;25"
    )
    frame = domain.parse_informe(_zip(SEP_2026, csv), SEP_2026, SHA)
    assert frame.height == 5
    assert domain.count_duplicate_keys(frame) == 2


def test_clean_file_has_no_duplicate_keys() -> None:
    frame = domain.parse_informe(_zip(SEP_2026, _v2()), SEP_2026, SHA)
    assert domain.count_duplicate_keys(frame) == 0


NOW = dt.datetime(2026, 9, 27, 8, 0, tzinfo=dt.UTC)


def _bronze(root: Path) -> pl.DataFrame:
    """Read the whole bronze table in a stable order.

    Args:
        root (Path): Warehouse root.

    Returns:
        pl.DataFrame: Every bronze row, sorted by the grain key.
    """
    return pl.read_delta(str(adapters.bronze_path(root))).sort(list(schema.BRONZE_KEY))


def test_rerunning_a_month_leaves_bronze_identical(tmp_path: Path) -> None:
    sep = domain.parse_informe(_zip(SEP_2026, _v2()), SEP_2026, SHA)
    nov = domain.parse_informe(_zip(NOV_2023, _v1()), NOV_2023, SHA)
    adapters.write_bronze(tmp_path, sep, SEP_2026)
    adapters.write_bronze(tmp_path, nov, NOV_2023)
    before = _bronze(tmp_path)

    adapters.write_bronze(tmp_path, sep, SEP_2026)

    after = _bronze(tmp_path)
    assert after.equals(before)
    assert after.height == sep.height + nov.height


def test_rewriting_a_month_never_touches_the_others(tmp_path: Path) -> None:
    """The predicate trap: an unscoped overwrite would wipe November here."""
    nov = domain.parse_informe(_zip(NOV_2023, _v1()), NOV_2023, SHA)
    adapters.write_bronze(tmp_path, nov, NOV_2023)
    adapters.write_bronze(
        tmp_path, domain.parse_informe(_zip(SEP_2026, _v2()), SEP_2026, SHA), SEP_2026
    )
    smaller_sep = domain.parse_informe(_zip(SEP_2026, _v2()), SEP_2026, "e" * 64).head(2)

    adapters.write_bronze(tmp_path, smaller_sep, SEP_2026)

    bronze = _bronze(tmp_path)
    assert bronze.filter(pl.col("reference_month") == SEP_2026).height == 2
    assert bronze.filter(pl.col("reference_month") == NOV_2023).height == nov.height


def _entry(sha256: str, etag: str, at: dt.datetime = NOW) -> dict[str, object]:
    """Build a manifest row for September 2026.

    Args:
        sha256 (str): Content fingerprint of the version.
        etag (str): Source ETag of the version.
        at (dt.datetime): Ingestion instant.

    Returns:
        dict[str, object]: Row matching the manifest schema.
    """
    return domain.manifest_entry(
        month=SEP_2026,
        sha256=sha256,
        remote=_remote(etag),
        row_count=5,
        duplicate_key_rows=0,
        layout_version=2,
        landing_path="landing/x.zip",
        now=at,
    )


def test_manifest_merge_is_idempotent(tmp_path: Path) -> None:
    adapters.record_version(tmp_path, _entry(SHA, '"a"'))
    adapters.record_version(tmp_path, _entry(SHA, '"a"'))
    assert pl.read_delta(str(adapters.manifest_path(tmp_path))).height == 1


def test_identical_republication_refreshes_the_etag(tmp_path: Path) -> None:
    adapters.record_version(tmp_path, _entry(SHA, '"old"'))
    later = NOW + dt.timedelta(days=1)

    adapters.refresh_version(tmp_path, SEP_2026, SHA, _remote('"new"'), later)

    current = adapters.current_version(tmp_path, SEP_2026)
    assert current == schema.ManifestEntry(SEP_2026, sha256=SHA, etag='"new"')
    assert pl.read_delta(str(adapters.manifest_path(tmp_path))).height == 1


def test_current_version_follows_a_to_b_to_a(tmp_path: Path) -> None:
    sha_a, sha_b = "a" * 64, "b" * 64
    adapters.record_version(tmp_path, _entry(sha_a, '"1"', NOW))
    adapters.record_version(tmp_path, _entry(sha_b, '"2"', NOW + dt.timedelta(days=1)))
    adapters.record_version(tmp_path, _entry(sha_a, '"3"', NOW + dt.timedelta(days=2)))

    assert adapters.current_version(tmp_path, SEP_2026).sha256 == sha_a
    assert pl.read_delta(str(adapters.manifest_path(tmp_path))).height == 2


def test_landing_is_write_once(tmp_path: Path) -> None:
    path = adapters.landing_path(tmp_path, SEP_2026, SHA)
    adapters.save_landing(path, b"first")
    adapters.save_landing(path, b"second")
    assert path.read_bytes() == b"first"
    assert not path.with_suffix(".partial").exists()
