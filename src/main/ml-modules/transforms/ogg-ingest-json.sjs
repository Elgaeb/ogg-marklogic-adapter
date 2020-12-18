function newBaseDocument({ schema, table }) {
    const document = {
        envelope: {
            headers: {
                columnUpdatedAtScn: {}
            },
            triples: [],
            instance: {},
        }
    }

    document.envelope.instance[schema] = {};
    document.envelope.instance[schema][table] = {};

    return document;
}

function getBaseDocument({ schema, table, uri }) {
    const document = cts.doc(uri);
    return (document != null) ? document.toObject() : newBaseDocument({ schema, table });
}

function highestScn(previous, current) {
    if(previous == null) {
        return current;
    } else if (current == null) {
        return previous;
    } else if(current > previous) {
        return current;
    } else {
        return previous;
    }
}

function ifNewer({ previous, current, doIfNewer = () => null, doIfOlder = () => null }) {
    if(previous == null || current == null || current > previous) {
        return doIfNewer(previous, current);
    } else {
        return doIfOlder(previous, current);
    }
}

exports.transform = function transform(context, params, content) {
    const root = content.toObject();
    if(root == null) {
        // probably a binary
        return content;
    }

    const uri = context.uri;
    const headers = root.envelope.headers;
    const { scn, operation, operationTimestamp, schema, table } = headers;

    const baseDocument = getBaseDocument({ schema, table, uri });

    const baseInstance = baseDocument.envelope.instance[schema][table];
    const baseHeaders = baseDocument.envelope.headers;
    const instance = root.envelope.instance[schema][table];

    baseHeaders.schema = schema;
    baseHeaders.table = table;
    baseHeaders.ingestedOn = fn.currentDateTime().toString();

    if(operation === "delete") {
        return doDelete({ scn, operation, operationTimestamp, instance, baseHeaders, baseInstance, baseDocument });
    } else {
        return doUpdate({ scn, operation, operationTimestamp, instance, baseHeaders, baseInstance, baseDocument });
    }
}

function doDelete({ scn, operation, operationTimestamp, instance, baseHeaders, baseInstance, baseDocument }) {
    return ifNewer({
        previous: baseHeaders.deletedAtScn,
        current: scn,
        doIfNewer: (previousDeletedScn, currentDeletedScn) => {
            baseHeaders.deletedAtScn = currentDeletedScn;

            copyInstance({ scn, instance, baseInstance, baseHeaders });
            Object.keys(baseHeaders.columnUpdatedAtScn).forEach(key => {
                ifNewer({
                    previous: baseHeaders.columnUpdatedAtScn[key],
                    current: currentDeletedScn,
                    doIfNewer: (columnScn, currentDeletedScn) => {
                        baseHeaders.columnUpdatedAtScn[key] = currentDeletedScn;
                        delete baseInstance[key];
                    },
                    doIfOlder: () => null
                });
            });

            ifNewer({
                previous: baseHeaders.scn,
                current: scn,
                doIfNewer: (previousScn, currentScn) => {
                    baseHeaders.operation = operation;
                    baseHeaders.scn = currentScn;
                    baseHeaders.operationTimestamp = operationTimestamp;
                    baseHeaders.deleted = true;
                },
                doIfOlder: (previousScn, currentScn) => null
            })

            return baseDocument;
        },
        doIfOlder: (previousDeletedScn, currentDeletedScn) => baseDocument
    })
}

function doUpdate({ scn, operation, operationTimestamp, instance, baseHeaders, baseInstance, baseDocument }) {
    ifNewer({
        previous: baseHeaders.scn,
        current: scn,
        doIfNewer: () => {
            baseHeaders.operation = operation;
            baseHeaders.scn = scn;
            baseHeaders.operationTimestamp = operationTimestamp;
        }
    })

    // only update values if the document hasn't beedn deleted in the future.
    ifNewer({
        previous: baseHeaders.deletedAtScn,
        current: scn,
        doIfNewer: () => {
            copyInstance({ scn, instance, baseInstance, baseHeaders });
            baseHeaders.deleted = false;
        }
    });

    return baseDocument;
}

function copyInstance({ scn, instance, baseInstance, baseHeaders }) {
    Object.keys(instance).forEach(key => {
        const value = instance[key];
        const oldScn = baseHeaders.columnUpdatedAtScn[key];
        if(scn == null || oldScn == null || oldScn < scn) {
            baseHeaders.columnUpdatedAtScn[key] = scn;
            baseInstance[key] = value;
        }
    });
}
