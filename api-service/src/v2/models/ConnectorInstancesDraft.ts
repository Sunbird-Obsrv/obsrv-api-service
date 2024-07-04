import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
  
export const ConnectorInstancesDraft = sequelize.define("connector_instances_draft", {
    id: {
        type: DataTypes.STRING,
        primaryKey: true,
        allowNull: false,
        autoIncrement: true
    },
    dataset_id: {
        type: DataTypes.STRING,
        allowNull: false
    },
    connector_id: {
        type: DataTypes.STRING,
        allowNull: false
    },
    data_format: {
        type: DataTypes.STRING,
        defaultValue: "json",
        allowNull: false
    },
    connector_config: {
        type: DataTypes.STRING,
        allowNull: false
    },
    operations_config: {
        type: DataTypes.JSON,
        defaultValue: {},
        allowNull: false
    },
    status: {
        type: DataTypes.ENUM("Draft", "Publishing", "Live", "Retired"),
        defaultValue: "Draft",
        allowNull: false
    },
    validation_config: {
        type: DataTypes.JSON,
        defaultValue: {}
    },
    created_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM"
    },
    updated_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM"
    },
    published_date: {
        type: DataTypes.NUMBER
    }
}, {
    tableName: "connector_instances_draft",
    timestamps: true,
    createdAt: "created_date",
    updatedAt: "updated_date"
})