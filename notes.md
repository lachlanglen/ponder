## Problem

- Due to the fact that the current polling task fetches the latest block and runs on a default interval of 1000ms, blocks will likely be missed/skipped on chains with sub-second block time.
- In order to catch all blocks, polling interval needs to be increased to <= average block time of chain being indexed
- For "fast" chains (e.g. Arbitrum), this means polling every 250ms or less
- Block/logs processing can take longer than 250ms, creating a backup or "drift" between the local head and actual head, without the possibility of catching up.

## Solution

Generally speaking, to solve the problem, you would implement something like the following on each poll event:
- determine block range to query RPC for, based on local chain head vs. actual chain head (DONE)
- Query RPC for logs in block range (DONE)
- Determine whether any logs match user-defined filters
- If match is found:
  - Fetch block via RPC and add its transactions, together with logs to event store via `EventStore.insertRealtimeBlock` method
  - Emit `realtimeCheckpoint` event
- Additionally, processing tasks could be optimized to improve performance.


## Final Thoughts
- Obviously this is a WIP and regrettably I'm not delivering any running code due to time constraints. Hopefully my understanding of the problem and solution, together with the small snippets of code added, can demonstrate my ability to tackle this problem.
- One of my challenges with reworking the code was the fact that much of the business logic - e.g. in `blockTaskWorker()` - is "lumped together" rather than being broken out into individual functions. If I were undertaking this task for real, I would try to reorganize this code where appropriate into separate logical components that would make it faster and easier to develop/refactor.


