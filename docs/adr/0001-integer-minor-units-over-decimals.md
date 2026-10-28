# ADR-0001: Integer Minor Units (`long`) Instead of Floating-Point or Decimal Types

## Context
Financial ledgers cannot tolerate binary floating-point representation errors (`double` / `float`) or implicit rounding semantics across intermediate calculations.

## Decision
We represent all monetary amounts as a 64-bit signed integer (`long amountMinor`) in the currency's ISO 4217 minor unit (e.g., euro cents for `EUR`) paired with a validated `Currency` value object, and perform all arithmetic exclusively via `Math.addExact`, `Math.subtractExact`, and `Math.negateExact`. In PostgreSQL, all monetary columns use `BIGINT`.

## Consequences
Addition and subtraction are exact hardware integer instructions with zero rounding ambiguity, any 64-bit overflow immediately raises `ArithmeticException` rather than wrapping silently, and database `SUM(amount_minor) = 0` constraints execute without scale-coercion overhead.
