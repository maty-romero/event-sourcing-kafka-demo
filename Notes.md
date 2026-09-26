# Event sourcing 
- [Podcast](https://youtu.be/V7vhSHqMxus)

### What it is? 
- 'Function' that derives state from past events
- Related to Accountns and Finance model or domain but not stricly just this one

### Why should we care about event-sourcing? 
- Information system -> "remember what happened"; 
1. Derive value or conclusions from past events - E.g footbal game, sales, proces-optimization 
    - Analytics, stadistics, etc -> Not having separation from Analytics & Functionality 
2. Training AI agents -> See if can reduce friction, or whatever 
3. System Integration -> Loose coupling integration. + Extensibility
    - e.g new projections of state, kick off some process, issue a triger, etc
    - Reusing code < Reusing data to get value 
4. No imposed query language, you can build it. 

### Techologies associated, to start implement it? 
- Concept can be applied to anything --> E.g having a file on disk and appending events
- Repository where you store events least intersting thing -> in a file, in a relational DB, 
    - Characteristics must have 
        1. Inmutable
        2. Be able to see events before adding next one (no duplicates)
        3. OCC --> Gives to option on how to deal with concurrency
            1. Do -> resolves later e.g Editing document concurrent users
            2. Only commit if the info saw user was the lastest. Like Check -> I want consistency e.g Financial transaction 

- Menu
    1. EventStoreDb - Index, OCC or similar, State 
     intermediary
        - Or EvidentDB
    2. What Indexes?
    3. Schema (standart)? 
        Cloud events .io ; Optional field Subject; Bitemporal fields 


## Main takeaways

- Event sourcing is at it's core a append-only file with an I/O handler like  Kafka.
- The immutable log is the state as a stream of events, accessed though a left fold, accumulating the state changes and producing the current state.
- While both message passing and event handling pass data between components, message passing involves sending messages to trigger actions or communicate between components—event passing only captures state changes.
- There is no query or usage pattern opposed,  the behavior of a system can be derived from it's output (although often still implemented for optimization).
- There is still techniques like optimistic concurrency control needed for managing faults.
	- We still need a write-ahead log + some mid-tear state manager, thats building indexes for intermediate states.