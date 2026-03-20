"""
MCP Server para MongoDB — Operações CRUD
Conecta ao MongoDB local e expõe tools para Create, Read, Update e Delete.
Transporte: stdio (padrão para integração com editores/IDEs).
"""

import json
from typing import Any

from bson import ObjectId
from bson.json_util import dumps as bson_dumps
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# ── Configuração ─────────────────────────────────────────────────────────────
MONGO_URI = "mongodb://localhost:27017"
DEFAULT_DATABASE = "mongodbVSCodePlaygroundDB"
DEFAULT_COLLECTION = "sales"

# ── Conexão MongoDB ──────────────────────────────────────────────────────────
client = MongoClient(MONGO_URI)

# ── MCP Server ───────────────────────────────────────────────────────────────
mcp = FastMCP("MongoDB CRUD Server")


# ── Helpers ──────────────────────────────────────────────────────────────────
def _get_collection(database: str | None = None, collection: str | None = None):
    """Retorna o objeto Collection do pymongo."""
    db_name = database or DEFAULT_DATABASE
    col_name = collection or DEFAULT_COLLECTION
    return client[db_name][col_name]


def _parse_json(text: str | None) -> dict | list:
    """Faz parse de uma string JSON, retornando dict/list vazio se None/vazio."""
    if not text or text.strip() == "":
        return {}
    return json.loads(text)


def _serialize(obj: Any) -> str:
    """Serializa resultado MongoDB (incluindo ObjectId/datetime) para JSON legível."""
    return bson_dumps(obj, indent=2, ensure_ascii=False)


def _to_object_id(id_str: str) -> ObjectId | str:
    """Tenta converter a string para ObjectId; se falhar, retorna a string original."""
    try:
        return ObjectId(id_str)
    except Exception:
        return id_str


# ── Tools ────────────────────────────────────────────────────────────────────


@mcp.tool()
def list_collections(database: str | None = None) -> str:
    """Lista todas as collections de um database.

    Args:
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
    """
    db_name = database or DEFAULT_DATABASE
    db = client[db_name]
    names = db.list_collection_names()
    return json.dumps(names, indent=2, ensure_ascii=False)


@mcp.tool()
def find_documents(
    query: str | None = None,
    projection: str | None = None,
    sort: str | None = None,
    limit: int = 20,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Busca documentos em uma collection com filtro, projeção e ordenação opcionais.

    Args:
        query: Filtro em formato JSON (ex: '{"item": "abc"}'). Vazio = todos.
        projection: Campos a retornar em JSON (ex: '{"item": 1, "price": 1}').
        sort: Ordenação em JSON (ex: '{"price": -1}' para preço decrescente).
        limit: Número máximo de documentos a retornar (default: 20).
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    filter_dict = _parse_json(query)
    proj_dict = _parse_json(projection) or None
    sort_list = list(_parse_json(sort).items()) if sort else None

    cursor = col.find(filter_dict, proj_dict)
    if sort_list:
        cursor = cursor.sort(sort_list)
    cursor = cursor.limit(limit)

    docs = list(cursor)
    return _serialize(docs)


@mcp.tool()
def find_document_by_id(
    document_id: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Busca um documento pelo seu _id.

    Args:
        document_id: O _id do documento (string ou ObjectId).
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    doc = col.find_one({"_id": _to_object_id(document_id)})
    if doc is None:
        return json.dumps({"error": f"Documento com _id '{document_id}' não encontrado."})
    return _serialize(doc)


@mcp.tool()
def insert_document(
    document: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Insere um único documento na collection.

    Args:
        document: Documento em formato JSON (ex: '{"item": "new", "price": 15}').
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    doc = _parse_json(document)
    result = col.insert_one(doc)
    return json.dumps({
        "acknowledged": result.acknowledged,
        "inserted_id": str(result.inserted_id),
    })


@mcp.tool()
def insert_documents(
    documents: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Insere múltiplos documentos na collection.

    Args:
        documents: Lista de documentos em JSON (ex: '[{"item": "a"}, {"item": "b"}]').
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    docs = _parse_json(documents)
    if not isinstance(docs, list):
        return json.dumps({"error": "O parâmetro 'documents' deve ser uma lista JSON."})
    result = col.insert_many(docs)
    return json.dumps({
        "acknowledged": result.acknowledged,
        "inserted_ids": [str(oid) for oid in result.inserted_ids],
    })


@mcp.tool()
def update_document(
    document_id: str,
    update: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Atualiza um documento pelo _id.

    Args:
        document_id: O _id do documento a ser atualizado.
        update: Operação de update em JSON (ex: '{"$set": {"price": 20}}').
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    update_dict = _parse_json(update)
    result = col.update_one({"_id": _to_object_id(document_id)}, update_dict)
    return json.dumps({
        "acknowledged": result.acknowledged,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
    })


@mcp.tool()
def update_documents(
    query: str,
    update: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Atualiza múltiplos documentos que correspondem ao filtro.

    Args:
        query: Filtro em JSON para selecionar os documentos (ex: '{"item": "abc"}').
        update: Operação de update em JSON (ex: '{"$set": {"price": 20}}').
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    filter_dict = _parse_json(query)
    update_dict = _parse_json(update)
    result = col.update_many(filter_dict, update_dict)
    return json.dumps({
        "acknowledged": result.acknowledged,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
    })


@mcp.tool()
def delete_document(
    document_id: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Remove um documento pelo _id.

    Args:
        document_id: O _id do documento a ser removido.
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    result = col.delete_one({"_id": _to_object_id(document_id)})
    return json.dumps({
        "acknowledged": result.acknowledged,
        "deleted_count": result.deleted_count,
    })


@mcp.tool()
def delete_documents(
    query: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Remove múltiplos documentos que correspondem ao filtro.

    Args:
        query: Filtro em JSON (ex: '{"item": "xyz"}'). ATENÇÃO: query vazio remove TODOS.
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    filter_dict = _parse_json(query)
    result = col.delete_many(filter_dict)
    return json.dumps({
        "acknowledged": result.acknowledged,
        "deleted_count": result.deleted_count,
    })


@mcp.tool()
def count_documents(
    query: str | None = None,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Conta documentos na collection, opcionalmente filtrados.

    Args:
        query: Filtro em JSON (ex: '{"item": "abc"}'). Vazio = conta todos.
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    filter_dict = _parse_json(query)
    count = col.count_documents(filter_dict)
    return json.dumps({"count": count})


@mcp.tool()
def aggregate(
    pipeline: str,
    database: str | None = None,
    collection: str | None = None,
) -> str:
    """Executa um pipeline de aggregation na collection.

    Args:
        pipeline: Pipeline em JSON (lista de estágios).
                  Ex: '[{"$match": {"item": "abc"}}, {"$group": {"_id": "$item", "total": {"$sum": "$quantity"}}}]'
        database: Nome do database (default: mongodbVSCodePlaygroundDB).
        collection: Nome da collection (default: sales).
    """
    col = _get_collection(database, collection)
    pipe = _parse_json(pipeline)
    if not isinstance(pipe, list):
        return json.dumps({"error": "O pipeline deve ser uma lista JSON de estágios."})
    results = list(col.aggregate(pipe))
    return _serialize(results)


# ── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="stdio")
