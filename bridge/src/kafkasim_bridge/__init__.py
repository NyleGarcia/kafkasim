"""kafkasim-bridge: stream real Kafka cluster state as kafkasim SimEvents.

The wire format is contracts/bridge-frame.schema.json; recordings are
contracts/event-log.schema.json. Same contract the in-browser engine emits.
"""

from .model import PartitionState, Snapshot, diff, manifest_from

__all__ = ["Snapshot", "PartitionState", "diff", "manifest_from"]
