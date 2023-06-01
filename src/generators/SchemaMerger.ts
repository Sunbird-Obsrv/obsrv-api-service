
export class SchemaMerger {

    mergeSchema(...schemas: any): Map<string, any> {
        return this.getMerger().mergeObjects(schemas);
    }

    private getMerger() {
        var jsonMerger = require("json-merger");
        return jsonMerger
    }
}