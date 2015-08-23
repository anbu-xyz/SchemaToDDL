package com.github.schematoddl

import groovy.transform.Immutable

/**
 * Created by anbu on 23/08/15.
 */
@Immutable
class DDLObject {
    String objectType
    String objectName
    List<String> ddls
}
