import Emittery from "emittery";
import pLimit from "p-limit";
import { hexToNumber, numberToHex, RpcLog } from "viem";

import type { LogFilter } from "@/config/logFilters";
import type { Network } from "@/config/networks";
import { QueueError } from "@/errors/queue";
import type { EventStore } from "@/event-store/store";
import type { Common } from "@/Ponder";
import { poll } from "@/utils/poll";
import { type Queue, createQueue } from "@/utils/queue";
import { range } from "@/utils/range";
import { startClock } from "@/utils/timer";

import { isMatchedLogInBloomFilter } from "./bloom";
import { filterLogs } from "./filter";
import {
  type BlockWithTransactions,
  type LightBlock,
  rpcBlockToLightBlock,
} from "./format";

type RealtimeSyncEvents = {
  realtimeCheckpoint: { timestamp: number };
  finalityCheckpoint: { timestamp: number };
  shallowReorg: { commonAncestorTimestamp: number };
  deepReorg: { detectedAtBlockNumber: number; minimumDepth: number };
  error: { error: Error };
};

type RealtimeBlockTask = BlockWithTransactions;
type RealtimeSyncQueue = Queue<RealtimeBlockTask>;

export class UnsafeRealtimeSyncService extends Emittery<RealtimeSyncEvents> {
  private common: Common;
  private eventStore: EventStore;
  private logFilters: LogFilter[];
  private network: Network;

  // Queue of unprocessed blocks.
  private queue: RealtimeSyncQueue;
  // Block number of the current finalized block.
  private finalizedBlockNumber = 0;
  // Local representation of the unfinalized portion of the chain.
  private blocks: LightBlock[] = [];
  // Function to stop polling for new blocks.
  private unpoll?: () => any | Promise<any>;

  constructor({
    common,
    eventStore,
    logFilters,
    network,
  }: {
    common: Common;
    eventStore: EventStore;
    logFilters: LogFilter[];
    network: Network;
  }) {
    super();

    this.common = common;
    this.eventStore = eventStore;
    this.logFilters = logFilters;
    this.network = network;

    this.queue = this.buildQueue();
  }

  setup = async () => {
    // Fetch the latest block for the network.
    const latestBlock = await this.getLatestBlock();
    const latestBlockNumber = hexToNumber(latestBlock.number);

    this.common.logger.info({
      service: "realtime",
      msg: `Fetched latest block at ${latestBlockNumber} (network=${this.network.name})`,
    });

    this.common.metrics.ponder_realtime_is_connected.set(
      { network: this.network.name },
      1
    );

    // Set the finalized block number according to the network's finality threshold.
    // If the finality block count is greater than the latest block number, set to zero.
    const finalizedBlockNumber = Math.max(
      0,
      latestBlockNumber - this.network.finalityBlockCount
    );
    this.finalizedBlockNumber = finalizedBlockNumber;

    // Add the latest block to the unfinalized block queue.
    // The queue won't start immediately; see syncUnfinalizedData for details.
    const priority = Number.MAX_SAFE_INTEGER - latestBlockNumber;
    this.queue.addTask(latestBlock, { priority });

    return { latestBlockNumber, finalizedBlockNumber };
  };

  start = async () => {
    // If an endBlock is specified for every log filter on this network, and the
    // latest end blcock is less than the finalized block number, we can stop here.
    // The service won't poll for new blocks and won't emit any events.
    const endBlocks = this.logFilters.map((f) => f.filter.endBlock);
    if (
      endBlocks.every(
        (endBlock) =>
          endBlock !== undefined && endBlock < this.finalizedBlockNumber
      )
    ) {
      this.common.logger.warn({
        service: "realtime",
        msg: `No realtime log filters found (network=${this.network.name})`,
      });
      this.common.metrics.ponder_realtime_is_connected.set(
        { network: this.network.name },
        0
      );
      return;
    }

    // If the latest block was not added to the queue, setup was not completed successfully.
    if (this.queue.size === 0) {
      throw new Error(
        `Unable to start. Must call setup() method before start().`
      );
    }

    // Fetch the block at the finalized block number.
    const stopClock = startClock();
    const finalizedBlock = await this.network.client.request({
      method: "eth_getBlockByNumber",
      params: [numberToHex(this.finalizedBlockNumber), false],
    });
    if (!finalizedBlock) throw new Error(`Unable to fetch finalized block`);
    this.common.metrics.ponder_realtime_rpc_request_duration.observe(
      { method: "eth_getBlockByNumber", network: this.network.name },
      stopClock()
    );

    this.common.logger.info({
      service: "realtime",
      msg: `Fetched finalized block at ${hexToNumber(
        finalizedBlock.number!
      )} (network=${this.network.name})`,
    });

    // Add the finalized block as the first element of the list of unfinalized blocks.
    this.blocks.push(rpcBlockToLightBlock(finalizedBlock));

    // The latest block was already added to the unfinalized block queue during setup(),
    // so here all we need to do is start the queue.
    this.queue.start();

    // Add an empty task the queue (the worker will fetch the latest block).
    // TODO: optimistically optimize latency here using filters or subscriptions.
    this.unpoll = poll(
      async () => {
        await this.addNewLatestBlocks();
      },
      { emitOnBegin: false, interval: this.network.pollingInterval }
    );
  };

  kill = async () => {
    this.unpoll?.();
    this.queue.pause();
    this.queue.clear();
    this.common.logger.debug({
      service: "realtime",
      msg: `Killed realtime sync service (network=${this.network.name})`,
    });
  };

  onIdle = async () => {
    await this.queue.onIdle();
  };

  private getLatestBlock = async () => {
    // Fetch the latest block for the network.
    const stopClock = startClock();
    const latestBlock_ = await this.network.client.request({
      method: "eth_getBlockByNumber",
      params: ["latest", true],
    });
    if (!latestBlock_) throw new Error(`Unable to fetch latest block`);
    this.common.metrics.ponder_realtime_rpc_request_duration.observe(
      { method: "eth_getBlockByNumber", network: this.network.name },
      stopClock()
    );
    return latestBlock_ as BlockWithTransactions;
  };

  private getLocalHead = () => {
    return this.blocks[this.blocks.length - 1];
  };

  // This method is only public for to support the tests.
  addNewLatestBlocks = async () => {
    // called by poll
    // get latest block
    const block = await this.getLatestBlock();
    // the lower the block number, the higher its priority
    const priority = Number.MAX_SAFE_INTEGER - hexToNumber(block.number);
    // get head of local chain
    const head = this.getLocalHead();
    // if the latest block is the same as the local head, do nothing
    if (head.hash === block.hash) {
      return;
    }
    // determine the range of blocks to fetch based on the local chain head and latest block
    const fromBlock = numberToHex(head.number);
    const toBlock = block.number;
    // fetch logs for block range
    const logs = await this.network.client.request({
      method: "eth_getLogs",
      params: [{ fromBlock, toBlock }],
    });
    console.log("logs: ", logs);
    // determine whether any logs are of interest
    // if they are, get individual block details where relevant
    // TODO: pass logs to task (rather than block)
    //
    this.queue.addTask(block, { priority });
  };

  private buildQueue = () => {
    const queue = createQueue<RealtimeBlockTask>({
      worker: async ({ task }: { task: RealtimeBlockTask }) => {
        await this.logsTaskWorker(task); // base task on logs rather than block, to take advantage of eth RPC's ability to fetch logs within block range
      },
      options: { concurrency: 1, autoStart: false },
      onError: ({ error, task }) => {
        const queueError = new QueueError({
          queueName: "Realtime sync queue",
          task: {
            hash: task.hash,
            parentHash: task.parentHash,
            number: task.number,
            timestamp: task.timestamp,
            transactionCount: task.transactions.length,
          },
          cause: error,
        });
        this.emit("error", { error: queueError });

        // Default to a retry (uses the retry options passed to the queue).
        // queue.addTask(task, { retry: true });
      },
    });

    return queue;
  };

  private findLogMatches = async (logs: RpcLog[]) => {
    // TODO: WIP; an attempt to break out logic to make it easier to work with
    // First, check if the new block _might_ contain any logs that match the registered filters.
    // skipping this, as we already have all the logs

    // Filter logs down to those that actually match the registered filters.
    const matchedLogs = filterLogs({
      logs,
      logFilters: this.logFilters.map((l) => l.filter),
    });
    const matchedLogCount = matchedLogs.length;
    const matchedLogCountText =
      matchedLogCount === 1
        ? "1 matched log"
        : `${matchedLogCount} matched logs`;

    // Filter transactions down to those that are required by the matched logs.
    // TODO: pick up here / finish
  };

  private logsTaskWorker = async (block: BlockWithTransactions) => {
    // fetch missing blocks, then fetch logs in between local head and actual head
    const previousHeadBlock = this.blocks[this.blocks.length - 1];

    // If no block is passed, fetch the latest block.
    const newBlockWithTransactions = block;
    const newBlock = rpcBlockToLightBlock(newBlockWithTransactions);

    // 1) We already saw and handled this block. No-op.
    if (this.blocks.find((b) => b.hash === newBlock.hash)) {
      this.common.logger.trace({
        service: "realtime",
        msg: `Already processed block at ${newBlock.number} (network=${this.network.name})`,
      });
      return;
    }

    // 2) This is the new head block (happy path). Yay!
    if (
      newBlock.number == previousHeadBlock.number + 1 &&
      newBlock.parentHash == previousHeadBlock.hash
    ) {
      this.common.logger.debug({
        service: "realtime",
        msg: `Started processing new head block ${newBlock.number} (network=${this.network.name})`,
      });

      // First, check if the new block _might_ contain any logs that match the registered filters.
      // await this.findLogMatches();
      const isMatchedLogPresentInBlock = isMatchedLogInBloomFilter({
        bloom: newBlockWithTransactions.logsBloom!,
        logFilters: this.logFilters.map((l) => l.filter),
      });

      if (!isMatchedLogPresentInBlock) {
        this.common.logger.debug({
          service: "realtime",
          msg: `No logs found in block ${newBlock.number} using bloom filter (network=${this.network.name})`,
        });
      }

      if (isMatchedLogPresentInBlock) {
        // If there's a potential match, fetch the logs from the block.
        const stopClock = startClock();
        const logs = await this.network.client.request({
          method: "eth_getLogs",
          params: [{ blockHash: newBlock.hash }],
        });
        this.common.metrics.ponder_realtime_rpc_request_duration.observe(
          { method: "eth_getLogs", network: this.network.name },
          stopClock()
        );

        // Filter logs down to those that actually match the registered filters.
        const matchedLogs = filterLogs({
          logs,
          logFilters: this.logFilters.map((l) => l.filter),
        });
        const matchedLogCount = matchedLogs.length;
        const matchedLogCountText =
          matchedLogCount === 1
            ? "1 matched log"
            : `${matchedLogCount} matched logs`;

        this.common.logger.debug({
          service: "realtime",
          msg: `Found ${logs.length} total and ${matchedLogCountText} in block ${newBlock.number} (network=${this.network.name})`,
        });

        // Filter transactions down to those that are required by the matched logs.
        const requiredTransactionHashes = new Set(
          matchedLogs.map((l) => l.transactionHash)
        );
        const filteredTransactions =
          newBlockWithTransactions.transactions.filter((t) =>
            requiredTransactionHashes.has(t.hash)
          );

        // If there are indeed any matched logs, insert them into the store.
        if (matchedLogCount > 0) {
          // TODO: THIS MATTERS
          await this.eventStore.insertRealtimeBlock({
            chainId: this.network.chainId,
            block: newBlockWithTransactions,
            transactions: filteredTransactions,
            logs: matchedLogs,
          });

          this.common.logger.info({
            service: "realtime",
            msg: `Found ${matchedLogCountText} in new head block ${newBlock.number} (network=${this.network.name})`,
          });
        } else {
          // If there are not, this was a false positive on the bloom filter.
          this.common.logger.debug({
            service: "realtime",
            msg: `Logs bloom for block ${newBlock.number} was a false positive (network=${this.network.name})`,
          });
        }
      }

      this.emit("realtimeCheckpoint", {
        timestamp: hexToNumber(newBlockWithTransactions.timestamp),
      });

      // Add this block the local chain.
      this.blocks.push(newBlock);
    }

    // 3) At least one block is missing.
    // Note that this is the happy path for the first task after setup, because
    // the unfinalized block range must be fetched (eg 32 blocks on mainnet).
    if (newBlock.number > previousHeadBlock.number + 1) {
      const missingBlockNumbers = range(
        previousHeadBlock.number + 1,
        newBlock.number
      );

      // Fetch all missing blocks using a request concurrency limit of 10.
      const limit = pLimit(10);

      const missingBlockRequests = missingBlockNumbers.map((number) => {
        return limit(async () => {
          const stopClock = startClock();
          const block = await this.network.client.request({
            method: "eth_getBlockByNumber",
            params: [numberToHex(number), true],
          });
          if (!block) {
            throw new Error(`Failed to fetch block number: ${number}`);
          }
          this.common.metrics.ponder_realtime_rpc_request_duration.observe(
            {
              method: "eth_getBlockByNumber",
              network: this.network.name,
            },
            stopClock()
          );
          return block as BlockWithTransactions;
        });
      });

      const missingBlocks = await Promise.all(missingBlockRequests);

      // Add blocks to the queue from oldest to newest. Include the current block.
      for (const block of [...missingBlocks, newBlockWithTransactions]) {
        const priority = Number.MAX_SAFE_INTEGER - hexToNumber(block.number);
        this.queue.addTask(block, { priority });
      }

      this.common.logger.info({
        service: "realtime",
        msg: `Fetched missing blocks [${missingBlockNumbers[0]}, ${
          missingBlockNumbers[missingBlockNumbers.length - 1]
        }] (network=${this.network.name})`,
      });

      return;
    }
  };

  // private blockTaskWorker = async (block: BlockWithTransactions) => {
  //   // TODO: consider receiving an array of blocks
  //   // fetch missing blocks, then fetch logs in between local head and actual head
  //   const previousHeadBlock = this.blocks[this.blocks.length - 1];

  //   // If no block is passed, fetch the latest block.
  //   const newBlockWithTransactions = block;
  //   const newBlock = rpcBlockToLightBlock(newBlockWithTransactions);

  //   // 1) We already saw and handled this block. No-op.
  //   if (this.blocks.find((b) => b.hash === newBlock.hash)) {
  //     this.common.logger.trace({
  //       service: "realtime",
  //       msg: `Already processed block at ${newBlock.number} (network=${this.network.name})`,
  //     });
  //     return;
  //   }

  //   // 2) This is the new head block (happy path). Yay!
  //   if (
  //     newBlock.number == previousHeadBlock.number + 1 &&
  //     newBlock.parentHash == previousHeadBlock.hash
  //   ) {
  //     this.common.logger.debug({
  //       service: "realtime",
  //       msg: `Started processing new head block ${newBlock.number} (network=${this.network.name})`,
  //     });

  //     // First, check if the new block _might_ contain any logs that match the registered filters.
  //     const isMatchedLogPresentInBlock = isMatchedLogInBloomFilter({
  //       bloom: newBlockWithTransactions.logsBloom!,
  //       logFilters: this.logFilters.map((l) => l.filter),
  //     });

  //     if (!isMatchedLogPresentInBlock) {
  //       this.common.logger.debug({
  //         service: "realtime",
  //         msg: `No logs found in block ${newBlock.number} using bloom filter (network=${this.network.name})`,
  //       });
  //     }

  //     if (isMatchedLogPresentInBlock) {
  //       // If there's a potential match, fetch the logs from the block.
  //       const stopClock = startClock();
  //       const logs = await this.network.client.request({
  //         method: "eth_getLogs",
  //         params: [{ blockHash: newBlock.hash }],
  //       });
  //       this.common.metrics.ponder_realtime_rpc_request_duration.observe(
  //         { method: "eth_getLogs", network: this.network.name },
  //         stopClock()
  //       );

  //       // Filter logs down to those that actually match the registered filters.
  //       const matchedLogs = filterLogs({
  //         logs,
  //         logFilters: this.logFilters.map((l) => l.filter),
  //       });
  //       const matchedLogCount = matchedLogs.length;
  //       const matchedLogCountText =
  //         matchedLogCount === 1
  //           ? "1 matched log"
  //           : `${matchedLogCount} matched logs`;

  //       this.common.logger.debug({
  //         service: "realtime",
  //         msg: `Found ${logs.length} total and ${matchedLogCountText} in block ${newBlock.number} (network=${this.network.name})`,
  //       });

  //       // Filter transactions down to those that are required by the matched logs.
  //       const requiredTransactionHashes = new Set(
  //         matchedLogs.map((l) => l.transactionHash)
  //       );
  //       const filteredTransactions =
  //         newBlockWithTransactions.transactions.filter((t) =>
  //           requiredTransactionHashes.has(t.hash)
  //         );

  //       // If there are indeed any matched logs, insert them into the store.
  //       if (matchedLogCount > 0) {
  //         // TODO: THIS MATTERS
  //         await this.eventStore.insertRealtimeBlock({
  //           chainId: this.network.chainId,
  //           block: newBlockWithTransactions,
  //           transactions: filteredTransactions,
  //           logs: matchedLogs,
  //         });

  //         this.common.logger.info({
  //           service: "realtime",
  //           msg: `Found ${matchedLogCountText} in new head block ${newBlock.number} (network=${this.network.name})`,
  //         });
  //       } else {
  //         // If there are not, this was a false positive on the bloom filter.
  //         this.common.logger.debug({
  //           service: "realtime",
  //           msg: `Logs bloom for block ${newBlock.number} was a false positive (network=${this.network.name})`,
  //         });
  //       }
  //     }

  //     this.emit("realtimeCheckpoint", {
  //       timestamp: hexToNumber(newBlockWithTransactions.timestamp),
  //     });

  //     // Add this block the local chain.
  //     this.blocks.push(newBlock);
  //   }

  //   // 3) At least one block is missing.
  //   // Note that this is the happy path for the first task after setup, because
  //   // the unfinalized block range must be fetched (eg 32 blocks on mainnet).
  //   if (newBlock.number > previousHeadBlock.number + 1) {
  //     const missingBlockNumbers = range(
  //       previousHeadBlock.number + 1,
  //       newBlock.number
  //     );

  //     // Fetch all missing blocks using a request concurrency limit of 10.
  //     const limit = pLimit(10);

  //     const missingBlockRequests = missingBlockNumbers.map((number) => {
  //       return limit(async () => {
  //         const stopClock = startClock();
  //         const block = await this.network.client.request({
  //           method: "eth_getBlockByNumber",
  //           params: [numberToHex(number), true],
  //         });
  //         if (!block) {
  //           throw new Error(`Failed to fetch block number: ${number}`);
  //         }
  //         this.common.metrics.ponder_realtime_rpc_request_duration.observe(
  //           {
  //             method: "eth_getBlockByNumber",
  //             network: this.network.name,
  //           },
  //           stopClock()
  //         );
  //         return block as BlockWithTransactions;
  //       });
  //     });

  //     const missingBlocks = await Promise.all(missingBlockRequests);

  //     // Add blocks to the queue from oldest to newest. Include the current block.
  //     for (const block of [...missingBlocks, newBlockWithTransactions]) {
  //       const priority = Number.MAX_SAFE_INTEGER - hexToNumber(block.number);
  //       this.queue.addTask(block, { priority });
  //     }

  //     this.common.logger.info({
  //       service: "realtime",
  //       msg: `Fetched missing blocks [${missingBlockNumbers[0]}, ${
  //         missingBlockNumbers[missingBlockNumbers.length - 1]
  //       }] (network=${this.network.name})`,
  //     });

  //     return;
  //   }
  // };
}
