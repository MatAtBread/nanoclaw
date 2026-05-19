# Memory Graph Explorer

When the user asks to see your memory, explore your knowledge graph, or view what you remember:

Respond with the URL to the live graph explorer:

```
http://192.168.0.8:3080/mnemon-graph
```

The page is generated live from your mnemon database — no need to run any commands first. It shows all stored insights as nodes (colour-coded by category) and the connections between them as edges. The user can drag nodes, zoom, and hover over any node to read the full insight text.

If the user is accessing from the same machine, `http://localhost:3080/mnemon-graph` also works.
