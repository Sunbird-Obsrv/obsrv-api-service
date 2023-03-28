import app from "../app";
import chai from "chai";
import chaiHttp from "chai-http";
import spies from "chai-spies";
import httpStatus from "http-status";
import constants from '../resources/Constants.json'
import { TestDataSource } from "./Fixtures";
import { config } from "./Config";
import { routesConfig } from "../configs/RoutesConfig";
import { dbConnector } from "../routes/Router";
import { Datasources } from "../helpers/Datasources";

chai.use(spies);
chai.should();
chai.use(chaiHttp);

describe("Datasource create API", () => {
    it("should insert a record in the database", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.resolve()
        })
        chai
            .request(app)
            .post(config.apiDatasourceSaveEndPoint)
            .send(TestDataSource.VALID_SCHEMA)
            .end((err, res) => {
                res.should.have.status(httpStatus.OK);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.save.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                res.body.result.message.should.be.eq(constants.CONFIG.DATASOURCE_SAVED)
                chai.spy.restore(dbConnector, "execute");
                done();
            });
    });
    it("should not insert record in the database", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.reject(new Error("error occured while connecting to postgres"))
        })
        chai
            .request(app)
            .post(config.apiDatasourceSaveEndPoint)
            .send(TestDataSource.VALID_SCHEMA)
            .end((err, res) => {
                res.should.have.status(httpStatus.INTERNAL_SERVER_ERROR);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["500_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.save.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                chai.spy.restore(dbConnector, "execute");
                done();
            });
    });
    it("should not insert record when request object contains missing fields", (done) => {
        chai
            .request(app)
            .post(config.apiDatasourceSaveEndPoint)
            .send(TestDataSource.MISSING_REQUIRED_FIELDS_CREATE)
            .end((err, res) => {
                res.should.have.status(httpStatus.BAD_REQUEST);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["400_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.save.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                done();
            });
    })
    it("should not insert record when given invalid schema", (done) => {
        chai
            .request(app)
            .post(config.apiDatasourceSaveEndPoint)
            .send(TestDataSource.INVALID_SCHEMA)
            .end((err, res) => {
                res.should.have.status(httpStatus.BAD_REQUEST);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["400_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.save.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                done();
            });
    })
})
describe("Datasource update API", () => {
    it("should successfully update records in database", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.resolve()
        })
        chai
            .request(app)
            .patch(config.apiDatasourceUpdateEndPoint)
            .send(TestDataSource.VALID_SCHEMA)
            .end((err, res) => {
                res.should.have.status(httpStatus.OK);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.update.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                res.body.result.message.should.be.eq(constants.CONFIG.DATASOURCE_UPDATED)
                chai.spy.restore(dbConnector, "execute")
                done();
            });
    });
    it("should not update records in database", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.reject(new Error("error occured while connecting to postgres"))
        })
        chai
            .request(app)
            .patch(config.apiDatasourceUpdateEndPoint)
            .send(TestDataSource.VALID_UPDATE_SCHEMA)
            .end((err, res) => {
                res.should.have.status(httpStatus.INTERNAL_SERVER_ERROR);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["500_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.update.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                chai.spy.restore(dbConnector, "execute")
                done();
            });
    });
    it("should not update records when request object does not contain required fields", (done) => {
        chai
            .request(app)
            .patch(config.apiDatasourceUpdateEndPoint)
            .send(TestDataSource.MISSING_REQUIRED_FIELDS_UPDATE)
            .end((err, res) => {
                res.should.have.status(httpStatus.BAD_REQUEST);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["400_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.update.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                done();
            });
    });

})
describe("Datasource read API", () => {
    it("should successfully retrieve records from database", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.resolve([TestDataSource.VALID_RECORD])
        })
        chai
            .request(app)
            .get(config.apiDatasourceReadEndPoint.replace(":datasourceId", TestDataSource.SAMPLE_ID).concat('?status=ACTIVE'))
            .end((err, res) => {
                res.should.have.status(httpStatus.OK);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.read.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                res.body.result.should.be.a("object")
                chai.spy.restore(dbConnector, "execute")
                done();
            })
    }),
        it("should throw error if records are empty", (done) => {
            chai.spy.on(dbConnector, "execute", () => {
                return Promise.resolve([])
            })
            chai
                .request(app)
                .get(config.apiDatasourceReadEndPoint.replace(":datasourceId", TestDataSource.SAMPLE_ID).concat('?status=ACTIVE'))
                .end((err, res) => {
                    res.should.have.status(httpStatus.NOT_FOUND);
                    res.body.should.be.a("object");
                    res.body.responseCode.should.be.eq(httpStatus["404_NAME"]);
                    res.body.should.have.property("result");
                    res.body.id.should.be.eq(routesConfig.config.datasource.read.api_id);
                    res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                    chai.spy.restore(dbConnector, "execute")
                    done();
                })
        }),
        it("should not retrieve records", (done) => {
            chai.spy.on(dbConnector, "execute", () => {
                return Promise.reject(new Error("error while connecting to postgres"))
            })
            chai
                .request(app)
                .get(config.apiDatasourceReadEndPoint.replace(":datasourceId", TestDataSource.SAMPLE_ID).concat('?status=ACTIVE'))
                .end((err, res) => {
                    res.should.have.status(httpStatus.INTERNAL_SERVER_ERROR);
                    res.body.should.be.a("object");
                    res.body.responseCode.should.be.eq(httpStatus["500_NAME"]);
                    res.body.should.have.property("result");
                    res.body.id.should.be.eq(routesConfig.config.datasource.read.api_id);
                    res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                    chai.spy.restore(dbConnector, "execute")
                    done();
                })
        })

})
describe("Datasource list API", () => {
    it("should successfully list records in the table", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.resolve([{}])
        })
        chai
            .request(app)
            .post(config.apiDatasourceListEndPoint)
            .send(TestDataSource.VALID_LIST_REQUEST_DISABLED_STATUS)
            .end((err, res) => {
                res.should.have.status(httpStatus.OK);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.list.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                res.body.result.should.be.a("array")
                chai.spy.restore(dbConnector, "execute")
                done();
            });
    })
    it("should not list records if request object is invalid", (done) => {
        chai
            .request(app)
            .post(config.apiDatasourceListEndPoint)
            .send({ "filters": { "status": "ACTIVE" }, "offset": true })
            .end((err, res) => {
                res.should.have.status(httpStatus.BAD_REQUEST);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["400_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.list.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                done();
            });
    })
    it("should not list records", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.reject(new Error("error while connecting to postgres"))
        })
        chai
            .request(app)
            .post(config.apiDatasourceListEndPoint)
            .send(TestDataSource.VALID_LIST_REQUEST_ACTIVE_STATUS)
            .end((err, res) => {
                res.should.have.status(httpStatus.INTERNAL_SERVER_ERROR);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["500_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.list.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                chai.spy.restore(dbConnector, "execute")
                done();
            })
    })
    it("it should query on draft table if status field is not provided", (done) => {
        chai.spy.on(dbConnector, "execute", () => {
            return Promise.resolve([{}, {}, {}])
        })
        chai
            .request(app)
            .post(config.apiDatasourceListEndPoint)
            .send({ "filters": { "status": ["ACTIVE"] } })
            .end((err, res) => {
                res.should.have.status(httpStatus.OK);
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.list.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                res.body.result.should.be.a("array")
                res.body.result.should.have.length(3)
                chai.spy.restore(dbConnector, "execute")
                done();
            });
    })
})
describe("Datasource PRESET API", () => {
    it("should return default params", (done) => {
        chai
            .request(app)
            .get(config.apiDatasourcePresetEndPoint)
            .end((err, res) => {
                res.should.have.status(httpStatus.OK)
                res.body.should.be.a("object");
                res.body.responseCode.should.be.eq(httpStatus["200_NAME"]);
                res.body.should.have.property("result");
                res.body.id.should.be.eq(routesConfig.config.datasource.preset.api_id);
                res.body.params.status.should.be.eq(constants.STATUS.SUCCESS)
                done()
            })
    })
        ,
        it("should handle errors", (done) => {
            chai.spy.on(Datasources.prototype, "getDefaults", () => { throw new Error("Test error") })
            chai
                .request(app)
                .get(config.apiDatasourcePresetEndPoint)
                .end((err, res) => {
                    res.should.have.status(httpStatus.INTERNAL_SERVER_ERROR)
                    res.body.should.be.a("object");
                    res.body.responseCode.should.be.eq(httpStatus["500_NAME"]);
                    res.body.should.have.property("result");
                    res.body.id.should.be.eq(routesConfig.config.datasource.preset.api_id);
                    res.body.params.status.should.be.eq(constants.STATUS.FAILURE)
                    chai.spy.restore(Datasources.prototype, "getDefaults")
                    done()
                })
        })
})