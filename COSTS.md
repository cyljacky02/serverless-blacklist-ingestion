# Costs

This file summarizes the current cost model for the `blacklist-ingestion` stack in `ap-east-2`.

## Validated Sources

The figures below were validated using:

- live stack executions in `ap-east-2`
- AWS Price List API checks performed on 2026-03-26
- official AWS pricing pages:
  - [AWS Lambda pricing](https://aws.amazon.com/lambda/pricing/)
  - [Amazon API Gateway pricing](https://aws.amazon.com/api-gateway/pricing/)
  - [Amazon DynamoDB pricing](https://aws.amazon.com/dynamodb/pricing/)
  - [AWS Step Functions pricing](https://aws.amazon.com/step-functions/pricing/)
  - [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/)

## Current Architecture Cost Shape

Main recurring cost components:

- AWS Lambda duration and request count
- AWS Step Functions Standard state transitions
- DynamoDB on-demand reads and writes
- S3 PUT/GET/LIST requests and retained object storage
- API Gateway REST requests for lookup traffic
- CloudWatch Logs and X-Ray tracing

Default operational guardrails now in place:

- Lambda log retention: `14 days`
- Lambda tracing mode: `PassThrough`

Main cost components that are **not** present in this stack today:

- NAT Gateway
- VPC endpoints / PrivateLink
- API Gateway cache
- WAF
- CloudFront
- custom domain and Route 53 records

## Current Regional Unit Prices

Region validated: `ap-east-2` / `Asia Pacific (Taipei)`

- Lambda duration: about `$0.000015 / GB-second`
- Lambda request: about `$0.00000018 / request`
- DynamoDB on-demand write: about `$0.6435 / million WRUs`
- DynamoDB on-demand read: about `$0.1602 / million RRUs`
- API Gateway REST requests: about `$3.825 / million requests`
- S3 Standard PUT/COPY/POST/LIST: about `$0.00423 / 1,000 requests`
- S3 Standard storage: about `$0.0225 / GB-month`
- Step Functions Standard: official pricing page reference of `$0.000025 / state transition`

## Runtime Modes

### 1. Full bootstrap / first cloud alignment

Latest live publish of the run-scoped model on 2026-03-26:

- execution `blacklist-delta-20260326053305`
- about `121s`
- dominated by the first full publish of `curated/runs/<run_id>/...` plus lookup-index population under the new schema

### 2. Delta-only steady state

Post-migration delta mode still depends on source churn:

- when at least one source changes, the workflow rebuilds the current run manifest and applies delta shards to the lookup table
- the latest live delta publish above wrote `138,209` current records, advanced the authoritative DynamoDB latest pointer, and refreshed the derived S3 latest pointer

### 3. All-sources-unchanged fast path

Two live control-path checks were validated on 2026-03-26:

- overlap guard:
  - execution `blacklist-overlap-20260326053350`
  - about `4.2s`
  - output ended with `skipped=true` and `skip_reason="run_in_progress"`
- forced rebuild path:
  - execution `blacklist-rebuild-20260326053440`
  - about `173s`
  - all sources were unchanged, but `lookup_sync_mode="rebuild"` forced merge and full lookup reconciliation
  - rebuild deleted `48,705` stale or legacy lookup rows

## Current Observed Storage

The artifact layout changed from `curated/deltas/` to immutable `curated/runs/<run_id>/...`, so the old prefix-level size figures are no longer authoritative. The bucket now expires `curated/runs/` objects after `90` days.

## Practical Cost Estimates

### Steady-state no-change workflow run

After the new skip-merge choice:

- about `10.0s`
- about `$0.00030 / execution`

At two executions per day:

- about `$0.018 / month`

### Lookup API request

A single lookup call is extremely small:

- about `$0.000004` to `$0.000005 / lookup`

This is dominated by API Gateway REST request pricing; Lambda and DynamoDB are smaller contributors at current payload sizes.

## What Still Matters

The main remaining sources of avoidable cost are:

- CloudWatch Logs retention if left unbounded
- X-Ray tracing if you switch `LambdaTracingMode` back to `Active`
- high lookup traffic on REST API instead of HTTP API

## Recommended Next Cost Optimizations

If cost minimization becomes a priority, the next most valuable optimizations are:

1. Reduce or disable X-Ray tracing if not actively used.
2. Revisit the current `14`-day CloudWatch Logs retention if your cost or forensic requirements change.
3. Revisit API Gateway type if `ap-east-2` gains stable HTTP API support and lookup traffic becomes meaningful.
4. Add a lightweight periodic cost report or dashboard from CloudWatch metrics and Billing / Cost Explorer.
