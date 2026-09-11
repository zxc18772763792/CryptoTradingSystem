# Universe coverage audit

The primary price panel starts from the 530 Binance USDⓈ-M USDT perpetual contracts that were trading when the frozen baseline was collected. It includes listing dates from the saved ExchangeInfo snapshot and distinguishes listing age in validation.

Historical delisted contracts could not be reconstructed from the available official snapshot or existing cache with sufficient confidence. They are therefore not silently imputed into the primary universe. The research summary records zero recovered historical delisted symbols and labels the remaining issue as unresolved survivorship bias.

Three OI cohorts are kept separate in `oi_cohort_coverage.csv`:

1. the all-market price cohort;
2. the long-window cached OI/market-cap cohort used for incremental walk-forward testing;
3. the broad recent-30-day OI cohort, used only for coverage analysis and future annotations because it is too short for the full historical claim.

Futures-only versus spot-listed classification depends on a contemporaneous public Spot ExchangeInfo refresh. When external refresh is disabled, the field is retained but treated as unavailable rather than inferred.
