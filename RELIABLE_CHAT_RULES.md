# Reliable Ordered Chat Rules (Version 1)

**Status:** Approved for the project implementation on 19 Nov 2026.

This document specifies the version 1 rules for reliable, ordered group chat over UDP. It applies to chat delivery; it does not define file-transfer reliability.

## 1. Recipients and membership

When a peer accepts a new chat message, it takes a snapshot of its active peer list and sends one unicast `CHAT` datagram to every peer in that snapshot. A peer that joins afterward receives later messages, not earlier chat history. A peer that leaves or times out is removed from the set of recipients.

Each peer-to-peer recipient stream is independent. A slow or unresponsive recipient does not stop chat delivery to other recipients.

## 2. CHAT payload and size

The `CHAT` payload is:

```text
uint32 chat_sequence | UTF-8 text bytes
```

`chat_sequence` is a per-sender, per-recipient counter that begins at 1. The same group chat text may carry different `chat_sequence` values for different recipients.

The current receiver uses `recvfrom(1024)`. Version 1 therefore limits the complete application datagram to 1,024 bytes. With a 20-byte header and 4-byte chat sequence, UTF-8 text is limited to 1,000 bytes. The limit counts encoded bytes, not characters.

## 3. Sequence numbers

The header `seq_no` is a 16-bit counter for reliable datagrams sent in one direction between two peers. It is separate from the 32-bit `chat_sequence` in the payload. Each counter starts at 1 for a newly established peer-to-peer session.

Version 1 does not wrap either counter. Before either counter would exceed its field's maximum, the sender must leave and establish a new peer session using a new peer ID. The new session starts its counters at 1. Receivers discard the old session's pending chat, expected-sequence, and duplicate-tracking state when that peer is removed.

## 4. ACK format and meaning

An `ACK` has an empty payload. Its header `ack_no` is the exact `seq_no` of the reliable datagram it confirms. An ACK uses `seq_no = 0`; ACK packets are not acknowledged.

A receiver sends an ACK only after validating the datagram's version, checksum, header and payload lengths, and UTF-8 chat text. Invalid packets are discarded without an ACK.

## 5. Sending, retransmission, and delivery

For each recipient, the sender allows at most one unacknowledged CHAT datagram at a time. Later chat messages for that recipient wait in order until the outstanding message is acknowledged. The sender can send to other recipients independently.

If the sender has not received a matching ACK after 1 second, it retransmits the exact same datagram with the same `seq_no` and `chat_sequence`. It continues retrying while the recipient remains active. If the existing 15-second heartbeat timeout removes the recipient, the sender stops retrying and marks delivery to that peer as failed. Delivery to other active peers continues.

## 6. Duplicate handling and order

The receiver tracks the expected `seq_no` and `chat_sequence` for each sender-recipient session.

- A valid expected CHAT is ACKed and delivered to the chat display once. Both expected counters advance.
- A duplicate CHAT is ACKed again but is not displayed again and does not advance the counters.
- A CHAT with a later-than-expected sequence is not delivered or ACKed. The sender's retransmission of the missing datagram allows the stream to recover.
- A CHAT with an earlier sequence that is not a recognized duplicate is discarded without delivery.

Because the sender waits for each recipient's ACK before sending that recipient's next CHAT, later messages cannot overtake an earlier unacknowledged message in that stream.

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
  |     on 1-second timeout: resend same  |
  |     CHAT with unchanged sequence IDs  |
```

The flow repeats per recipient. ACKs are not ACKed. If the receiver sees a valid duplicate CHAT, it sends the matching ACK again and suppresses duplicate display.
