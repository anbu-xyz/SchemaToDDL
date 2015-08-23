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
        objectType=objectType?.toUpperCase()
        def objects=this.sourceConnection.rows([owner: this.sourceSchema,
                                                objectType: objectType], sql)
        List<DDLObject> allDDLs=[]
        for(def object: objects) {
            DDLObject ddlObject=new DDLObject(
                    objectName: object.OBJECT_NAME,
                    objectType: objectType,
                    ddls: [genericDDL(objectType, object.OBJECT_NAME)] as List<String>
            )
            allDDLs.add(ddlObject)
        }
        return allDDLs.asImmutable()
    }

    public ExtractDDL(Sql sourceConnection, String sourceSchema, ExtractDDLOptions options) {
        this.sourceConnection=sourceConnection
        this.sourceSchema=sourceSchema?.toUpperCase()
        this.options=options
    }

    public tableDDLs() {
        List<DDLObject> allTableDDLs=[]
        initConnectionProperties()
        def tables=sourceConnection.rows([owner: this.sourceSchema], """
            with fkrel as (
                SELECT a.table_name, a.owner, count(*) FKCOUNT
                FROM all_cons_columns a
                JOIN all_constraints c ON a.owner = c.owner
                AND a.constraint_name = c.constraint_name
                WHERE c.constraint_type = 'R'
                AND a.owner = :owner
                group by a.table_name, a.owner
            )
            SELECT t.object_name as TABLE_NAME FROM ALL_OBJECTS t
              LEFT OUTER JOIN fkrel on t.object_name=fkrel.table_name and t.owner = fkrel.owner
            WHERE (t.owner = :owner and t.object_type='TABLE')
                  AND (t.owner, t.object_name) NOT IN (
              SELECT owner, mview_name
              FROM all_mviews
              UNION ALL
              SELECT log_owner, log_table
              FROM all_mview_logs
            )
            group by t.object_name, t.owner, fkrel.FKCOUNT
            order by nvl(fkrel.FKCOUNT,0)
        """);
        for(def table:tables) {
            List ddlList=[]
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
                ddlList.add("COMMENT ON COLUMN \"${this.sourceSchema}\".\"${tableName}\".\"${comment.COLUMN_NAME}\" " +
                        "IS '${comment.COMMENTS.replaceAll("'", "''")}';")
            }
            def indexes=sourceConnection.rows([owner: this.sourceSchema, tableName: tableName], """
                 SELECT index_name as INDEX_NAME
                   FROM all_indexes
                  WHERE owner = :owner
                    AND table_name= :tableName
                 MINUS
                 SELECT constraint_name as INDEX_NAME
                   FROM all_constraints
                  WHERE owner = :owner
                   AND table_name = :tableName
                   AND constraint_type = 'P'
            """)
            for(def index: indexes) {
                ddlList.add(genericDDL('INDEX', index.INDEX_NAME))
            }
            def ddlObject=new DDLObject(objectName: table.TABLE_NAME?.toUpperCase(), objectType: 'TABLE', ddls: ddlList)
            allTableDDLs.add(ddlObject)
        }
        return allTableDDLs.asImmutable()
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
        List<DDLObject> fullDdlList=[]
        fullDdlList.addAll(extractDdl.tableDDLs())
        for(String type: ['VIEW', 'PROCEDURE', 'PACKAGE', 'SEQUENCE', 'SYNONYM',
                'TRIGGER', 'MATERIALIZED VIEW', 'LIBRARY', 'TYPE', 'FUNCTION']) {
            fullDdlList.addAll(extractDdl.genericDDLForType(type))
        }
        new DDLFileWriter().writeToDisk(fullDdlList, new File('/tmp/ddl'))
    }
}
