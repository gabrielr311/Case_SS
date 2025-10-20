import json
import time
import hashlib
from datetime import datetime
from typing import List, Dict, Callable, Optional, Any, Iterable

import numpy as np
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
from file_operations import PDFParser, PDFChunk
from minio_handler import MinioHandler
from logging import Logger
import formats as fmts

from sentence_transformers import SentenceTransformer
import torch


class OpensearchHandler:
    def __init__(self, 
                logger: Logger,
                embeddings_model_name : str,
                embeddings_dim :int,
                host: str,
                port: int,
                password: str,
                use_gpu_for_embeddings: bool = False
                ):
        self.logger = logger
        self.client = OpenSearch(hosts=[{"host": host, "port": port}],http_auth=("admin",password))
        
        self.embeddings_dim = embeddings_dim
        self.embedding_model_name = embeddings_model_name
        self.device = "cuda" if (use_gpu_for_embeddings and torch.cuda.is_available()) else "cpu"

    def get_embeddings_multi_qa(self,
        texts: List[str],
        batch_size: int = 64,
        device: Optional[str] = None,
        normalize: bool = True,
    ) -> List[List[float]]:
        """
        Compute embeddings for a list of texts using the SentenceTransformers model

        Args:
            texts: List of input strings to embed.
            model_name: Hugging Face / sentence-transformers model name (default: multi-qa-MiniLM-L6-dot-v1).
            batch_size: Batch size for encoding (tune for memory / speed).
            device: torch device string ("cpu" or "cuda"). If None, auto-detects.
            normalize: If True, returns L2-normalized vectors (useful for cosine similarity).

        Returns:
            List of embeddings (each embedding is a list of floats). Embedding dimension for
            the default model is 384.

        Example:
            embs = get_embeddings_multi_qa(["texto em português", "outra frase"])
            # embs -> [[0.123, ...], [0.234, ...]]
        """
        model = SentenceTransformer(self.embedding_model_name, device=device)

        # sanitize inputs and compute embeddings in batches
        cleaned_texts: List[str] = []
        original_to_idx = []  # map to keep position of empty strings
        for i, t in enumerate(texts):
            if t is None:
                cleaned_texts.append("")
                original_to_idx.append(i)
            else:
                # basic normalization: strip and collapse whitespace
                s = " ".join(str(t).split())
                cleaned_texts.append(s)
                original_to_idx.append(i)

        embeddings = []
        for start in range(0, len(cleaned_texts), batch_size):
            batch = cleaned_texts[start : start + batch_size]
            batch_emb = model.encode(batch, show_progress_bar=False, convert_to_numpy=True)
            if normalize:
                # L2-normalize (common for cosine similarity)
                norms = np.linalg.norm(batch_emb, axis=1, keepdims=True)
                norms[norms == 0] = 1.0
                batch_emb = batch_emb / norms
            # convert to python floats
            for vec in batch_emb:
                embeddings.append(vec.astype(float).tolist())

        # safety: embeddings length should match inputs
        if len(embeddings) != len(texts):
            # This should not happen, but fail loudly if mismatch
            raise RuntimeError(f"Embeddings count {len(embeddings)} != texts count {len(texts)}")

        return embeddings

    # ---------- helpers ----------
    def ensure_embedding_list(self,emb: Any) -> List[float]:
        """
        Ensure embedding is a plain Python list of floats.
        Accepts numpy arrays, lists, etc.
        """
        if emb is None:
            return None
        if isinstance(emb, np.ndarray):
            return emb.astype(float).tolist()
        if isinstance(emb, list):
            return [float(x) for x in emb]
        # fallback
        return json.loads(json.dumps(emb))

    # ---------- index utilities ----------
    def ensure_index(self, index_name: str):
        """
        Create the index with recommended mapping if it doesn't exist.
        Adjust settings (shards/replicas) to your environment.
        """
        if self.client.indices.exists(index=index_name):
            # optionally validate mapping/knn dimension
            mapping = self.client.indices.get_mapping(index=index_name)[index_name]["mappings"]
            props = mapping.get("properties", {})
            if "embedding" in props:
                emb_field = props["embedding"]
                # if mapping uses knn_vector and has dimension, validate
                dim = None
                if emb_field.get("type") == "knn_vector":
                    dim = emb_field.get("dimension")
                if dim and dim != self.embeddings_dim:
                    raise ValueError(f"Index '{index_name}' already exists with embedding dimension {dim}, expected {self.embeddings_dim}")
            return

        body = {
            "settings": {
                "index.knn": True,
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "1s",
            },
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},                     # Exact match, not analyzed
                    "chunk_id": {"type": "keyword"},                   # Unique per chunk
                    "confidence": {"type": "float"},                   # Confidence score from OCR/ML
                    "read_method": {"type": "keyword"},                # Method used to read the chunk
                    "text": {"type": "text","analyzer": "standard"},   # Full text, analyzed for search
                    "text_raw": {"type": "keyword"},                   # Small snippet for exact matching
                    "page_start": {"type": "integer"},                 # Page start index
                    "page_end": {"type": "integer"},                   # Page end index
                    "tokens": {"type": "integer"},                     # Token count in chunk
                    "ingestion_ts": {"type": "date"},                  # Timestamp when ingested
                    "source": {"type": "keyword"},                     # Source system or bucket
                    "file_hash": {"type": "keyword"},                  # File hash for deduplication
                    "trace_id": {"type": "keyword"},                   # Trace/logging id
                    "document_type": {"type": "keyword"},              # Type/category of document
                    "ref_date": {"type": "date"},                      # Reference date of document
                    "aggregation_type": {"type": "keyword"},           # Optional, CVM ITR and DPF specific
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": self.embeddings_dim,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {"ef_construction": 256, "m": 48}
                        }
                    }
                }
            }
        }
        self.client.indices.create(index=index_name, body=body, ignore=400)
        # optionally wait for green
        self.client.cluster.health(wait_for_status="yellow", timeout=60)


    # ---------- main ingestion ----------
    def ingest_chunks_to_opensearch(self,
        index_name: str,
        file_data_and_chunks: Dict,
        file_metadata : Dict[str,str],
        embedding_fn: Optional[Callable[[List[str]], List[List[float]]]] = None,
        set_refresh_back: bool = True,
        max_retries: int = 3,
        force_reindex : bool = False,
    ):
        """
        Ingest chunk dicts into OpenSearch.

        - client: OpenSearch client
        - index_name: target index
        - chunks: iterable of chunk dicts (see expected format above)
        - embedding_fn: function that accepts a list of texts and returns list of embeddings (lists/floats)
        """

        bucket_file_path = file_data_and_chunks["stats"]["bucket_file_path"]

        self.logger.info(f"Starting to ingest file '{bucket_file_path}' chunks into OpenSearch's index '{index_name}'.Embeddings model: '{self.embedding_model_name}', dimension: '{self.embeddings_dim}', device: '{self.device}'")

        # 1) ensure index exists and check embedding dimension
        self.ensure_index(index_name)

        chunks = file_data_and_chunks["chunks"]

        search_results = self.client.search(index=index_name, body={"query":{"term": {"doc_id.keyword": bucket_file_path}}},size=500)

        chunks_found_for_file = [hit["_id"] for hit in search_results['hits']['hits']]

        if not force_reindex and (len(chunks_found_for_file) == len(chunks)):
            self.logger.warning(f"All {len(chunks_found_for_file)} chunks for file '{bucket_file_path}' are already present at OpenSearch's index '{index_name}'. Skipping this file's indexing...")
            return

        # 2) temporarily speed up bulk by pausing refresh
        prev_settings = None
        try:
            prev_settings = self.client.indices.get_settings(index=index_name)[index_name]["settings"]["index"].get("refresh_interval", None)
            self.client.indices.put_settings(index=index_name, body={"index": {"refresh_interval": "-1"}})
        except Exception:
            # not fatal, continue
            pass

        # 3) iterate in batches, compute embeddings if needed, bulk index
        batch = []
        docs_in_batch = []
        total = 0

        def flush_batch(b):
            nonlocal total
            if not b:
                return
            tries = 0
            while tries < max_retries:
                try:
                    # helpers.bulk handles newline separated operations efficiently
                    success, info = helpers.bulk(self.client, b, stats_only=False, raise_on_error=False)
                    total += success
                    break
                except Exception as exc:
                    tries += 1
                    self.logger.error(f"bulk failed (attempt {tries}/{max_retries}): {exc}")
                    time.sleep(2 ** tries)
            b.clear()

        def make_opensearch_doc(chunk: PDFChunk, i: int) -> Dict:

            base_doc = {
                "doc_id": bucket_file_path,
                "chunk_id": f"{bucket_file_path}#chunk_{i}",
                "confidence":  chunk.confidence,
                "read_method":  chunk.method,
                "text": chunk.text,
                "text_raw": chunk.text[:512],  # small exact-match token, trimmed to avoid huge keyword fields
                "page_start": chunk.page_start,
                "page_end": chunk.page_end,
                "tokens": chunk.tokens,
                "ingestion_ts": file_metadata[fmts.BucketCustomMetadata.INGEST_TS.value],
                "source": file_metadata[fmts.BucketCustomMetadata.SOURCE.value],
                "file_hash": file_metadata[fmts.BucketCustomMetadata.FILE_HASH.value],
                "trace_id": file_metadata[fmts.BucketCustomMetadata.TRACE_ID.value],
                "document_type": file_metadata[fmts.BucketCustomMetadata.DOCUMENT_TYPE.value],
                "ref_date": file_metadata[fmts.BucketCustomMetadata.REF_DATE.value],
                "aggregation_type": file_metadata.get(fmts.BucketCustomMetadata.AGGREGATION_TYPE.value,""), # Only CVM's ITR and DFP have aggregation data as stated in fmts.CVMDocumentAggregationType
                # embedding to be attached below
            }
            return base_doc

        chunk_iter = iter(chunks)
        i_global = 0
        text_batch = []
        doc_batch = []

        amt_of_chunks = len(chunks)
        curr_chunk = 0
        while True:
            try:
                chunk = next(chunk_iter)
                curr_chunk+=1
                self.logger.info(f"Processing file '{bucket_file_path}' chunk: {curr_chunk}/{amt_of_chunks}" )
            except StopIteration:
                # flush any remaining
                if doc_batch:
                    # compute embeddings for the last batch if required
                    # if embedding_fn:
                    #     embeddings = embedding_fn([d["text"] for d in doc_batch])
                    # else:
                    embeddings = [d.get("embedding") for d in doc_batch]
                    # attach and prepare bulk actions
                    for j, doc in enumerate(doc_batch):
                        emb = self.ensure_embedding_list(embeddings[j]) if embeddings else None
                        doc_body = doc.copy()
                        if emb is not None:
                            doc_body["embedding"] = emb
                        # action meta with routing
                        action = {
                            "_op_type": "index",
                            "_index": index_name,
                            "_id": doc["chunk_id"],
                            "_source": doc_body,
                            "routing": doc["doc_id"]
                        }
                        batch.append(action)
                    flush_batch(batch)
                break

            # normalization
            doc = make_opensearch_doc(chunk, i_global)
            doc_batch.append(doc)
            text_batch.append(doc["text"])
            i_global += 1

            # when we have a minibatch for embeddings or reached batch_size for bulk
            #if len(doc_batch) >= min(batch_size, 200):  # embed in smaller chunks for memory reasons
            embeddings = self.get_embeddings_multi_qa([d["text"] for d in doc_batch])
            # attach embeddings and append to bulk payload
            for j, d in enumerate(doc_batch):
                emb = self.ensure_embedding_list(embeddings[j]) if embeddings else None
                d_body = d.copy()
                if emb is not None:
                    d_body["embedding"] = emb
                action = {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": d["chunk_id"],
                    "_source": d_body,
                    "routing": d["doc_id"]
                }
                batch.append(action)
            # flush batch (helpers.bulk expects iterable of actions)
            flush_batch(batch)
            doc_batch = []
            text_batch = []

        # 4) restore refresh interval and optionally refresh the index
        try:
            if prev_settings is not None and set_refresh_back:
                self.client.indices.put_settings(index=index_name, body={"index": {"refresh_interval": prev_settings}})
            elif set_refresh_back:
                self.client.indices.put_settings(index=index_name, body={"index": {"refresh_interval": "1s"}})
            # force a refresh so documents are queryable immediately
            self.client.indices.refresh(index=index_name)
        except Exception:
            pass

        self.logger.info(f"File '{bucket_file_path}' indexed with success on OpenSearch's index '{index_name}' with '{amt_of_chunks}' chunks")
        return total
