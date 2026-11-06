# Determinism and recovery

`scripts/recovery-test.sh` runs `DomainHourTotals` five times over the same input and compares each
output with the first by sha256. The input is the `user` files of 1 and 28 October 2026: eleven
HDFS blocks, 110,180,010 lines, so the maps run in two waves on the cluster's eight task slots.

| Run | What is killed |
|---|---|
| clean | nothing |
| rerun | nothing |
| worker | a whole worker container: its DataNode, its NodeManager and the task JVMs on it, a node failure |
| datanode | only a worker's DataNode process |
| nodemanager | only a worker's NodeManager process |

The kill lands when the map phase passes 30%, on the worker running the most containers at that
moment. After each failure the daemon comes back and the script waits for three live DataNodes,
three NodeManagers and no under-replicated block before the next run. For the tests the cluster
notices a dead daemon in about a minute (`cluster/hadoop.env`) instead of the default ten.

## First round: two runs failed

The clean run, the rerun and the NodeManager run gave identical output. The worker and DataNode
runs exited with an error, although YARN recorded both applications as succeeded and both had
written a complete output, with `_SUCCESS`, whose sha256 equalled the clean run's.

The ApplicationMaster's log said why:

```
Failed to replace a bad datanode on the existing pipeline due to no more good datanodes being
available to try.
```

While a job runs, its ApplicationMaster writes the job's history to HDFS. When a DataNode in that
write pipeline dies, the HDFS client tries to replace it with another DataNode. On three DataNodes at
replication 3 there is no other: the third copy already sits on each of the two that remain. By
default the write then fails. The history file was left broken, the JobHistory server could not
find the job, and the job's client, asking it for the final status, gave up with exit 1.

The data was never at risk: the map tasks read their blocks from the two remaining replicas, and
the single reducer wrote its output to a pipeline that did not include the dead node. What failed
was bookkeeping, which is how a small cluster usually fails.

## The fix

```
dfs.client.block.write.replace-datanode-on-failure.best-effort=true
```

With best-effort, a writer that cannot replace a dead DataNode carries on with the ones left, and
the NameNode brings the block back to three copies once a DataNode returns. Hadoop's own description
of the replacement policy advises this, or turning replacement off, for clusters of three nodes or
fewer.

## Second round: all five identical

Same input, same kill point, with the fix:

| Run | Victim | Map progress at the kill | Map attempts launched | Failed | Killed | Exit | Output |
|---|---|---|---|---|---|---|---|
| clean | | | 12 | 0 | 1 | 0 | `3deb1d52…` |
| rerun | | | 12 | 0 | 1 | 0 | identical |
| worker | worker3 | 35.5% | 14 | 2 | 1 | 0 | identical |
| datanode | worker3 | 35.3% | 11 | 0 | 0 | 0 | identical |
| nodemanager | worker3 | 34.8% | 15 | 0 | 4 | 0 | identical |

Full sha256 of the output: `3deb1d52bf1019f11b8e0bd2e14e7b0a35da6bca0dc2b90db6d1d02448beb366`.

What each failure cost:

- **A whole worker.** Two map attempts failed with the node and ran again elsewhere. Every attempt
  still read its block locally, from a node that held a replica.
- **A DataNode alone.** No task failed: each reader that lost its DataNode moved to another replica
  of the same block mid-read.
- **A NodeManager alone.** Four attempts were killed and launched again, because YARN no longer
  had a record of the containers on that node once it declared the node lost.

The killed attempts in the clean run and the rerun are speculative duplicates, launched for a slow
map and killed when the original finished. The output does not depend on which attempt wins: Hadoop
commits one attempt per task.

## What this does not show

- **Re-replication after a lost node.** With three DataNodes at replication 3, a dead DataNode leaves
  each of its blocks with two copies and no third node to copy them to. The cluster stays
  under-replicated until the node returns, which is what the tests wait for. Healing without the
  node needs a fourth DataNode.
- **Real hostnames.** When a worker container is killed, Docker stops resolving its name, so other
  daemons log `UnknownHostException` for it. A real dead machine still resolves.
- **The victim varied little.** worker3 happened to run the most containers at each kill.
