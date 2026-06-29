# Event Sourcing Kafka Demo

A simple distributed system demonstrating the Event Sourcing pattern using Apache Kafka as an event store.

## Overview

Traditional systems typically store only the current state of an entity. In contrast, Event Sourcing stores every change as an immutable event, allowing the current state to be reconstructed from the event history.

This project implements a simplified banking domain where accounts can:

- Be created
- Receive deposits
- Perform withdrawals

Instead of persisting account balances directly, all operations are stored as events in Kafka.
