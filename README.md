# AWS RDS Snapshot
Lambda function to Copy live RDS snapshots to a backup for safe-keeping.

## Background
Disaster recovery (DR) is often thought of in terms of handling massive failures of infrastructure - the
loss of a whole data centre for example.
These kinds of failures are already mitigated by spreading the RDS instances across multiple availability zones.
However, there are other kinds of 'disasters' including the accidental or wilfull wholesale deletion of resources
by people who have legitimate access to AWS account.

The Lambda functions provided here offer a way of taking a daily copy of the most recent automated snapshots from
one or more RDS instances in a Live account to a Failsafe account and can be used as part
of the above strategy.
## Overview
Two Lambda python functions are provided.

The first, rdscopysnappshots.py, runs in the Live account on a daily
basis. It deletes any snapshot copies left over from the previous day's run, creates a new manual copy
of the most recent snapshot for each RDS instance, shares it with the Failsafe account and sends an SNS alert
to indicate that the snapshot is available.

The second function, rdssavesnapshot.py, runs in the Failsafe account and is triggered on receipt of the SNS alert.
It takes a manual copy of the newly shared snapshot and deletes any existing snapshots that are older
than a set age.
## Configuration
The following variables should be configured before use:
### rdscopysnapshot.py
| Variable | Description |
|----------|-------------|
| INSTANCES | A list of RDS instance identifiers for which snapshot copies are to be taken, e.g. ["db-name1", "db-name2"] |
| REGION | The AWS region in which the RDS instances exist, e.g. "eu-west-1" |
| SHAREWITH | The Failsafe account with which snapshots will be shared, e.g. "012345678901" |
| SNSARN | The SNS topic ARN used to announce availability of the manual snapshot copy, e.g.  "arn:aws:sns:eu-west-1:012345678901:rds-copy-snapshots" |
### rdssavesnapshot.py
| Variable | Description |
|----------|-------------|
| REGION | The AWS region in which the database instances exist, e.g.  "eu-west-1" |
| RETENTION | The snapshot retention period in days, e.g. 31 |

## Encryption
These functions have been tested with unencrypted RDS instances. They should work with encrypted RDS instances as well.
However, you will need to share the appropriate KMS
encryption key from the Live account to the Failsafe account prior to use,
as described at http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ShareSnapshot.html.
