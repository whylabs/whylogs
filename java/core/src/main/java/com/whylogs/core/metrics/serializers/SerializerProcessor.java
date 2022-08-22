package com.whylogs.core.metrics.serializers;

import com.whylogs.core.message.MetricComponentMessage;
import lombok.NoArgsConstructor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.function.Function;

@NoArgsConstructor
@SupportedAnnotationTypes("com.whylogs.core.metrics.serializers.BuiltInSerializer")
public class SerializerProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Method method : roundEnv.getElementsAnnotatedWith(BuiltInSerializer.class).getClass().getMethods()) {
            String name = method.getAnnotation(BuiltInSerializer.class).name();
            int typeId = method.getAnnotation(BuiltInSerializer.class).typeID();

            if(method.getReturnType() != MetricComponentMessage.class) {
                processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, "Serializer method must return MetricComponentMessage");
                return false;
            }

            if(method.getParameterCount() != 1) {
                processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, "Serializer method must have one parameter");
                return false;
            }

            if(!Modifier.isStatic(method.getModifiers()) || !Modifier.isPublic(method.getModifiers())) {
                processingEnv.getMessager().printMessage(javax.tools.Diagnostic.Kind.ERROR, "Serializer method must be public static");
                return false;
            }

            Type parameter = method.getParameters()[0].getType();

            if(!name.isEmpty()){
                SerializerRegistry.getInstance().register(name,  p -> SerializerRegistry.invokeMethod(method, method.getParameters()[0]));
            }

            if(typeId != -1){
                SerializerRegistry.getInstance().register(typeId,  p -> SerializerRegistry.invokeMethod(method, method.getParameters()[0]));
            }



        }
        return true;
    }
}
