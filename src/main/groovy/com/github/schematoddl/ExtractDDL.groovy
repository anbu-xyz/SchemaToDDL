package com.github.schematoddl

import groovy.sql.Sql

/**
 * Created by anbu on 22/08/15.
 */
class ExtractDDL {

    Sql sourceConnection
    String sourceSchema
    ExtractDDLOptions options

    private initConnectionProperties() {
        sourceConnection.execute("""
            begin
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SQLTERMINATOR', TRUE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'STORAGE', FALSE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'TABLESPACE', FALSE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', FALSE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'CONSTRAINTS_AS_ALTER', FALSE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'PARTITIONING', FALSE);
                dbms_metadata.set_transform_param( dbms_metadata.session_transform, 'PRETTY', TRUE);
             end;
        """);

    }

    private genericDDL(String objectType, String objectName) {
        String sql="""
           SELECT DBMS_METADATA.GET_DDL(replace(object_type, ' ', '_'), object_name, owner) as DDL
             FROM ALL_OBJECTS
            WHERE (owner = :owner and object_type=:objectType and object_name=:objectName)
        """
        def ddl=this.sourceConnection.firstRow([owner: this.sourceSchema,
                                    objectType: objectType?.toUpperCase(),
                                    objectName: objectName?.toUpperCase()], sql)
        return ddl?.DDL?.asciiStream?.text
    }

    private genericDDLForType(String objectType) {
        String sql="""
            SELECT object_name FROM ALL_OBJECTS WHERE (owner = :owner and object_type=:objectType)
        """
        def objects=this.sourceConnection.rows([owner: this.sourceSchema,
                                                objectType: objectType?.toUpperCase()], sql)
        def ddlStrings=[:]
        for(def object: objects) {
            ddlStrings.put(object.OBJECT_NAME, genericDDL(objectType, object.OBJECT_NAME))
        }
        println ddlStrings
    }

    public ExtractDDL(Sql sourceConnection, String sourceSchema, ExtractDDLOptions options) {
        this.sourceConnection=sourceConnection
        this.sourceSchema=sourceSchema?.toUpperCase()
        this.options=options
    }

    public tableDDLs() {
        def ddlStrings=[:]
        initConnectionProperties()
        def tables=sourceConnection.rows([owner: this.sourceSchema], """
            SELECT object_name as TABLE_NAME FROM ALL_OBJECTS WHERE (owner = :owner and object_type='TABLE')
                      AND (owner, object_name) NOT IN (
                           SELECT owner, mview_name
                             FROM all_mviews
                           UNION ALL
                           SELECT log_owner, log_table
                             FROM all_mview_logs
                      )
        """);
        def ddlList=[]
        for(def table:tables) {
            String tableName=table.TABLE_NAME?.toUpperCase()
            ddlList.add(genericDDL('TABLE', tableName))
            def comments=sourceConnection.rows([owner: this.sourceSchema, tableName: tableName], """
                 SELECT column_name, comments
                   FROM all_col_comments
                  WHERE owner=:owner
                    AND table_name=:tableName
                    AND comments is not null
            """);
            for(def comment: comments) {
                ddlList.add("COMMENT ON COLUMN ${this.sourceSchema}.${tableName}.${comment.COLUMN_NAME} " +
                        "IS '${comment.COMMENTS.replaceAll("'", "''")}';")
            }
            def indexes=sourceConnection.rows([owner: this.sourceSchema, tableName: tableName], """
                 SELECT index_name
                   FROM all_indexes
                  WHERE owner = :owner
                    AND table_name= :tableName
            """)
            for(def index: indexes) {
                ddlList.add(genericDDL('INDEX', index.INDEX_NAME))
            }
            ddlStrings.put(tableName, ddlList)
        }
        return ddlStrings
    }

    static void main(String[] args) {
        String host="192.168.56.101"
        String port="1521"
        String databasename="xe"
        String user="hr"
        def conn=Sql.newInstance("jdbc:oracle:thin:@$host:$port:$databasename",
                user, "oracle", "oracle.jdbc.OracleDriver")
        ExtractDDLOptions options=new ExtractDDLOptions(includeSchemaName: true)
        def extractDdl=new ExtractDDL(conn, 'hr', options)
        extractDdl.tableDDLs()
        for(String type: ['VIEW', 'PROCEDURE', 'PACKAGE', 'SEQUENCE', 'SYNONYM',
                'TRIGGER', 'MATERIALIZED VIEW', 'LIBRARY', 'TYPE', 'FUNCTION']) {
            extractDdl.genericDDLForType(type)
        }
    }
}
