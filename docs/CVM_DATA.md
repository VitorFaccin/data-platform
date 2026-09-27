# CVM data — what the source contains

A reading guide to the CVM (Comissão de Valores Mobiliários) open data this platform
ingests, written from the files themselves. Numbers were measured on the August 2026
files; they move month to month, the shapes do not.

## Daily fund reports — *Informe Diário* (`FI/DOC/INF_DIARIO`)

Every investment fund class in Brazil reports, for each business day, its size and flows.
One zip per month; ingested by [`bronze_cvm_fund_daily`](../dags/cvm/bronze_fund_daily/README.md).

| Column (source name) | Meaning | Type (CVM dictionary) |
|---|---|---|
| `TP_FUNDO_CLASSE` | fund/class type (`CLASSES - FIF`, `CLASSE FIF/FAPI`, `FI`) | varchar(20) |
| `CNPJ_FUNDO_CLASSE` | CNPJ of the fund class, **formatted** (`00.017.024/0001-53`) | varchar(18) |
| `ID_SUBCLASSE` | subclass id, empty for most classes | varchar(15) |
| `DT_COMPTC` | business day the figures refer to | date |
| `VL_TOTAL` | total portfolio value | numeric(17,2) |
| `VL_QUOTA` | value of one quota — the basis for returns | numeric(27,12) |
| `VL_PATRIM_LIQ` | net assets (*patrimônio líquido*) | numeric(17,2) |
| `CAPTC_DIA` | money in (subscriptions) that day | numeric(17,2) |
| `RESG_DIA` | money out (redemptions) that day | numeric(17,2) |
| `NR_COTST` | number of shareholders | int |

August 2026 in numbers: 536,671 rows · 25,563 fund classes · 21 business days ·
27.8 million shareholders on the last day · R$ 1.74 tri subscribed and R$ 1.70 tri
redeemed over the month.

What a row does **not** carry: the fund's name, strategy, manager or benchmark. Those
live in the registry below, which is why the two datasets meet in gold.

## Fund registry — *Registro de Fundos e Classes* (`FI/CAD/DADOS/registro_fundo_classe.zip`)

The post-CVM 175 registry: three related files, a snapshot overwritten daily (no
history — the platform has to build it, SCD2).

```
registro_fundo      (90k rows)  one fund       — manager (Gestor), administrator, status
   │ ID_Registro_Fundo
   ▼
registro_classe     (37k rows)  one fund class — classification, ANBIMA class, benchmark,
   │ ID_Registro_Classe                          target audience, fund-of-funds flag
   ▼
registro_subclasse  (10k rows)  one subclass   — audience, pension/exclusive flags
```

Columns that matter for analysis:

| File | Column | Why |
|---|---|---|
| `registro_classe` | `CNPJ_Classe` | join key to the daily reports — **digits only** here |
| `registro_classe` | `Classificacao` | Renda Fixa · Multimercado · Ações · Cambial · FMP-FGTS |
| `registro_classe` | `Classificacao_Anbima` | finer strategy classification |
| `registro_classe` | `Indicador_Desempenho` | the benchmark the class declares (10k classes: "DI de um dia", i.e. CDI; 1.3k: Ibovespa; IPCA and NTN-B indices) |
| `registro_classe` | `Classe_Cotas` | the class invests in other funds' quotas (a FIC) |
| `registro_classe` | `Publico_Alvo`, `Situacao` | retail / qualified / professional; active or not |
| `registro_fundo` | `Gestor`, `CPF_CNPJ_Gestor` | the asset manager — the natural "who" of the analysis |
| `registro_fundo` | `Administrador`, `CNPJ_Administrador` | the administrator |

The legacy `cad_fi.csv` (47k rows, one flat file) is still published but predates the
class/subclass model; the platform reads `registro_fundo_classe.zip`.

## Traps worth knowing before modeling

- **The CNPJ has two spellings.** Daily reports: `00.017.024/0001-53`; registry:
  `00017024000153`. Silver conforms both to 14 digits. With that, 99.4% of the classes in
  the August reports match a registry class.
- **Summing net assets double counts.** A fund of funds (FIC) holds quotas of other funds,
  so their money appears twice: a naive sum of August's last day gives R$ 14.2 tri, above
  the industry's real size. Industry totals must exclude `Classe_Cotas` classes (or
  consolidate them) — a gold rule, stated where it is applied.
- **Before 2023-12 the CNPJ identifies a fund, after it a fund class** (CVM Resolution
  175). Adapted funds usually kept their CNPJ on the main class, which keeps the series
  continuous in most cases — to be verified per fund when history matters.
- **The source repeats rows** (7 of the first 15 monthly files): byte-identical lines, and
  funds reported under two types during their CVM 175 transition. Bronze keeps them;
  silver resolves them.
- **Negative net assets and quotas exist** (hundreds of rows a month). Real, rare, and
  worth flagging rather than dropping.
- **Registry files carry accented text** (`IMOBILIÁRIO`), unlike the ASCII daily reports:
  their parser must assert the encoding instead of reusing the daily-report assumption.
