# Reliable Ordered Chat Rules (Version 2)

**Status:** Approved for the project implementation on 19 Nov 2026.

This document specifies the version 2 rules for reliable, ordered group chat over UDP. It applies to chat delivery; it does not define file-transfer reliability.

## 1. Recipients and membership

When a peer accepts a new chat message, it takes a snapshot of its active peer list and sends one unicast `CHAT` datagram to every peer in that snapshot. A peer that joins afterward receives later messages, not earlier chat history. A peer that leaves or times out is removed from the set of recipients.

Each peer-to-peer recipient stream is independent. A slow or unresponsive recipient does not stop chat delivery to other recipients.

## 2. CHAT payload and size

The `CHAT` payload is:

```text
uint32 session_id | uint32 chat_sequence | UTF-8 text bytes
```

`chat_sequence` is a per-sender, per-recipient counter that begins at 1. The same group chat text may carry different `chat_sequence` values for different recipients. It orders that recipient's messages from this sender.

The current receiver uses `recvfrom(1024)`. Version 2 therefore limits the complete application datagram to 1,024 bytes. With a 20-byte header, a 4-byte session ID and a 4-byte chat sequence, UTF-8 text is limited to 996 bytes. The limit counts encoded bytes, not characters.

## 3. Sequence numbers

The header `seq_no` is a 16-bit counter for CHAT datagrams sent in one direction between two peers. It is separate from the 32-bit `chat_sequence` in the payload. Both counters start at 1 for a sender's session with one recipient. They are per recipient and do not wrap. Other version 2 packet types use zero for both header sequence fields.

Neither counter wraps. Each stream has a random nonzero `session_id`. When a sender removes a recipient, it discards its outgoing stream; a later stream to the same peer gets a new session ID and starts both counters at 1. A receiver keeps its expected counters per sender and session ID, even when it removes the sender, and resets them only when a new session ID arrives. A datagram from a session the receiver has already replaced is discarded. This prevents the two failures of version 1, where a one-sided timeout either blocked a stream permanently or ACKed new messages as duplicates without displaying them.

## 4. ACK format and meaning

An `ACK` has an empty payload. Its header `ack_no` is the exact `seq_no` of the reliable datagram it confirms. An ACK uses `seq_no = 0`; ACK packets are not acknowledged.

A receiver sends an ACK only after validating the datagram's version, checksum, header and payload lengths, and UTF-8 chat text. Invalid packets are discarded without an ACK.

## 5. Sending, retransmission, and delivery

For each recipient, the sender may have up to eight unacknowledged CHAT datagrams in flight. The sender fills the available per-recipient window in sequence order. Each recipient stream is independent, so a slow peer does not block chat delivery to the other peers.

The initial retransmission timeout (RTO) is 1 second. The peer estimates round-trip time from ACKs for datagrams that were not retransmitted, updates SRTT and RTTVAR, and sets `RTO = SRTT + 4 * RTTVAR`, clamped to 0.2–3 seconds. After a timeout, that datagram's timeout doubles up to 3 seconds. The sender retransmits the exact same datagram with unchanged `seq_no` and `chat_sequence` until it receives the matching ACK or the recipient is removed. Other datagrams in the window may still be acknowledged and delivered independently.

Heartbeats are sent every 2 seconds. A peer with no heartbeat for 6 seconds is removed; its queued and pending chat is cleared and retries stop. Delivery to other active peers continues.

## 6. Duplicate handling and order

The receiver tracks the expected `seq_no` and `chat_sequence` for each sender-recipient session.

- A valid expected CHAT is ACKed and delivered to the chat display once. Both expected counters advance.
- A duplicate CHAT is ACKed again but is not displayed again and does not advance the counters.
- A CHAT with a later-than-expected sequence is not delivered or ACKed. The sender's retransmission of the missing datagram allows the stream to recover.
- A CHAT with an earlier sequence that is not a recognized duplicate is discarded without delivery.

The receiver advances its expected packet and chat counters only when it accepts the next expected CHAT. Later messages arriving early are discarded without ACK; their sender retries them after the missing earlier message has been acknowledged and delivered. This preserves the accepted chat order despite the eight-packet window.

## 7. Peer failures and guarantee boundary

When a peer leaves normally or is removed after the heartbeat timeout, it is no longer considered an active recipient. The sender clears that peer's pending chat state and stops retransmissions to it.

The delivery guarantee is: every peer that remains active and eventually receives a retransmission gets each sender's accepted chat message once and in order. A peer removed as failed may miss messages sent while it was unavailable.

## 8. Message flow

```text
Sender                                 Receiver
  |                                       |
  |-- CHAT(seq_no, chat_sequence, text) ->|
  |                                       | validate packet
  |                                       | if expected: display once
  |<-- ACK(ack_no = CHAT.seq_no) ----------|
  |                                       |
  |     on per-peer RTO: resend same CHAT |
  |     with unchanged sequence IDs       |
```

The sender can keep at most eight CHAT datagrams pending per recipient. The initial RTO is 1 second; subsequent RTOs adapt to ACK timing and are bounded to 0.2–3 seconds with exponential backoff. The flow repeats independently per recipient. ACKs are not ACKed. If the receiver sees a valid duplicate CHAT, it sends the matching ACK again and suppresses duplicate display. Heartbeat interval is 2 seconds and peer timeout is 6 seconds.
