package com.github.schematoddl

/**
 * Created by anbu on 23/08/15.
 */
class DDLFileWriter {
    static writeToDisk(List<DDLObject> ddlObjectList, File directory) {
        if(!directory.exists()) {
            directory.mkdirs()
        } else {
            if(!directory.isDirectory()) {
                throw new RuntimeException((directory+" is not a directory!").toString())
            }
        }
        def prefixSize=Integer.toString(ddlObjectList.size()).length()
        def prefixFormat="%0${prefixSize}d".toString()
        ddlObjectList.eachWithIndex { DDLObject ddlObject, Integer index ->
            def subDir=new File(directory, ddlObject.objectType)
            if(!subDir.exists()) subDir.mkdir()
            def ddlFile=new File(subDir, "${sprintf(prefixFormat, index)}-${ddlObject.objectName}.sql")
            if(ddlFile.exists()) ddlFile.delete()
            ddlFile.withWriter { file ->
                for(String s:ddlObject.ddls) {
                    file << s
                    file << "\n"
                }
            }
        }
    }

}
