# AI/ML Pattern Guides

These guides show how to build common AI application patterns using NodeDB's existing engines. Each guide includes production-ready SQL examples.

NodeDB provides the storage and retrieval layer. Your application handles chunking, embedding, reranking, and LLM generation. The boundary is clean: we store, index, search, and fuse. You chunk, embed, rerank, and generate.

## Retrieval

- [RAG Pipelines](rag-pipelines.md) — Basic RAG, hybrid vector+BM25, filtered retrieval, parent-document, conversational RAG
- [GraphRAG](graphrag.md) — Entity extraction, seed retrieval + graph expansion, community summarization, disambiguation
- [Multi-Modal Search](multi-modal-search.md) — Multiple vector columns, cross-modal CLIP search, multi-modal RRF fusion, ColBERT multi-vector

## Agent Architecture

- [Agent Memory](agent-memory.md) — Episodic (conversation logs), semantic (distilled facts), working (KV + TTL), scheduled consolidation

## Platform

- [On-Device AI](on-device.md) — NodeDB-Lite vector search, offline RAG, CRDT sync, WASM deployment, privacy

## Coming Soon

- [Feature Store](../columnar.md) — Columnar engine for training features, point-in-time lookups, batch export
- [CDC for Inference Triggers](../real-time.md) — Change streams for embedding pipelines and knowledge graph re-indexing
- [Evaluation Tracking](../timeseries.md) — Experiment metrics, retriever comparison, drift detection
- [Multi-Tenancy for AI SaaS](../security.md) — WAL-level tenant isolation, RLS during vector search, per-tenant budgets
