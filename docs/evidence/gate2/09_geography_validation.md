# 09 - Geography Validation

## Historical and intermediate states

Geographic alert counts changed during correction and regeneration:

- original historical observation: 26
- intermediate revised state: 22
- pre-floating-point-fix state: 24
- current validated state: 25

Historical counts are retained as historical observations and are not
recreated artificially.

## Floating-point threshold defect

Nyendo had a disbursement peer z-score represented as approximately:

`-0.9999999999999998`

with repayment peer z-score:

`1.0`

A mathematically boundary-equivalent value was therefore classified LOW
instead of MEDIUM because of floating-point representation.

Threshold comparisons were corrected using rounded z-score values at the
comparison boundary.

Final validated parish risk distribution:

- LOW: 13
- MEDIUM: 25
- total parishes: 38

Nyendo:

- disbursement z-score: approximately -1.0
- repayment z-score: 1.0
- final risk band: MEDIUM
- score: 2

Current geographic alerts:

- alerts: 25
- unique alert parishes: 25
- missing district ISO: 0
- missing coordinates: 0

## Geography hierarchy test

The Gold geography dimension stops at:

region -> district -> county -> sub-county

The earlier assertion incorrectly expected parish/village at this dimension
grain.

The assertion was corrected to the implemented hierarchy.

Final relevant geography tests passed, including:

- deterministic geography hierarchy
- Gold geography not-null/relationship/uniqueness checks
- parish performance
- parish geographic risk
- accepted risk-band values
- geographic alerts
- geographic coverage
- risk drivers
- local-government performance

## Attribution

Parish finance attribution follows:

loans -> Gold SACCO -> Silver SACCO office region/district/parish

Attribution semantic:

`SACCO_OFFICE_ATTRIBUTION`

Identity aggregates are not available at parish/village grain.

Village output is limited to agent coverage where supported.
