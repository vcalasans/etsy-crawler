def upsert(doc):
    d = doc.to_dict(True)
    d['_op_type'] = 'update'
    d['doc'] = d['_source']
    d['doc_as_upsert'] = True
    del d['_source']

    return d
