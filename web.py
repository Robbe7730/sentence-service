import os
import logging
import json
import uuid

from string import Template
from flask import request, Response

from helpers import logger, update
from escape_helpers import sparql_escape_uri

import nltk
nltk.download('punkt')

SENTENCE_URI_BASE = os.environ["SENTENCE_URI_BASE"]

logger.info(f"{SENTENCE_URI_BASE=}")

@app.route("/.mu/delta", methods=["POST"])
def delta():
    logger.debug(f"Got delta with data {request.data}")

    data = request.json

    if not data:
        return Response("Invalid data", 400)

    text_uris = []
    values = {}

    for delta in data:
        for tup in delta["inserts"]:
            if tup["predicate"]["value"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" and tup["object"]["value"] == "http://www.ontologydesignpatterns.org/ont/dul/IOLite.owl#Text":
                text_uris.append(tup["subject"]["value"])
            if tup["predicate"]["value"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#value":
                values[tup["subject"]["value"]] = tup["object"]["value"]

    # TODO: Here, we could check if a URI has a value/type in the database that's not in the delta
    # TODO: Check if we have already processed this text

    logger.debug(f"Found {len(text_uris)} iol:Text and {len(values)} rdf:value")

    if len(text_uris) == 0:
        return Response("No iol:Text found", 200)

    for uri in text_uris:
        if uri not in values:
            logger.error(f"No value found for {uri}")
            continue

        for sent in process_text(values[uri]):
            save_sentence(uri, sent)

    return Response("OK", 200)

def save_sentence(text_uri: str, value: str):
    sentence_uuid = str(uuid.uuid4()).replace("-", "").upper()
    sentence_uri = SENTENCE_URI_BASE + sentence_uuid
    query_template = Template("""
    PREFIX iol: <http://www.ontologydesignpatterns.org/ont/dul/IOLite.owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

    INSERT DATA {
        $sentence a iol:Sentence;
                  rdf:value $value;
                  mu:uuid $uuid.

        $text iol:hasComponent $sentence.
    }
    """)
    query_string = query_template.substitute(
        sentence=sparql_escape_uri(sentence_uri),
        value=sparql_escape(value),
        text=sparql_escape_uri(text_uri),
        uuid=sparql_escape(sentence_uuid)
    )
    update(query_string)

def process_text(text: str) -> [str]:
    ret = nltk.sent_tokenize(text)    
    logger.debug(f"Split text into {len(ret)} sentences")
    return ret
