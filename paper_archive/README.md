# paper_archive

Record populations supporting figures reported in the article that predate the
repository's continuous archiving.

The instrument's LLM verification layer entered service on 15 September 2026, and
daily archiving of matched record populations began on 16 September 2026. The
diagnostic window reported in Appendix B (7–13 September 2026) therefore has no
daily audit log or record file in the repository's ordinary output.

`matched_events_<start>_to_<end>.csv` restores the matched event population for
that window directly from `gdelt-bq.gdeltv2.events_partitioned`, using the same
actor-pair matching rule as the live pipeline, with country codes retained so the
bilateral assignment of every record can be checked independently.

`window_summary_<start>_to_<end>.json` reports event and mention counts by
partner under two date definitions (partition time and event date), the
negative mention-weighted signal that forms the denominator of the percentages
reported in Appendix B, and the twenty highest-impact South Korea–Japan records
with their source URLs.

Verification verdicts from the original diagnostic are not reproduced here: those
were model judgements made at the time, and re-running them under the current
prompt would not recover the same decisions. What is restored is the population
from which the reported percentages were computed.

Regenerate with `archive_paper_window.py` (see the repository workflow of the
same name).
