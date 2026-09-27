# plugins/

Custom operators, hooks and macros registered with Airflow's plugin manager.

Empty on purpose: no pipeline so far needs an operator that the providers
shipping with `apache/airflow:3.3.1` do not already cover. A plugin is *earned* by a
concrete need appearing twice — it is not scaffolding to fill in advance.
