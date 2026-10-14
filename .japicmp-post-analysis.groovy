// japicmp post-analysis filter — see the japicmp-maven-plugin configuration in pom.xml.
//
// An annotation element added WITH a default value is an additive change: nobody
// implements an annotation type, and every existing usage keeps compiling and
// linking. japicmp nevertheless reports it as METHOD_ABSTRACT_ADDED_TO_CLASS, which
// is source-incompatible for a regular class — so that one change is dropped here,
// for annotation types only and only when the element carries a default.
//
// Everything else stays: a new abstract method on an SPI interface or on a testkit
// contract class, an element added WITHOUT a default, a removal, a signature change
// — those are exactly what the gate is for.

import japicmp.model.JApiClassType
import japicmp.model.JApiCompatibilityChangeType

for (jApiClass in jApiClasses) {
    def newType = jApiClass.getClassType().getNewTypeOptional()
    if (!newType.isPresent() || newType.get() != JApiClassType.ClassType.ANNOTATION) {
        continue
    }
    for (method in jApiClass.getMethods()) {
        def newMethod = method.getNewMethod()
        if (!newMethod.isPresent()) {
            continue
        }
        boolean hasDefault = newMethod.get().getMethodInfo().getAttribute("AnnotationDefault") != null
        if (!hasDefault) {
            continue
        }
        for (change in method.getCompatibilityChanges()) {
            if (change.getType() == JApiCompatibilityChangeType.METHOD_ABSTRACT_ADDED_TO_CLASS) {
                // Kept in the report, reclassified as the additive change it is.
                change.setBinaryCompatible(true)
                change.setSourceCompatible(true)
            }
        }
    }
}
return jApiClasses
