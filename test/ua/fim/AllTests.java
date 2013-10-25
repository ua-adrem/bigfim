package ua.fim;

import java.lang.reflect.Field;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({FimDriverTest.class, ua.fim.bigfim.AllTests.class, ua.fim.eclat.util.AllTests.class})
public class AllTests {
  
  public static void setField(Object target, String fieldname, Object value) throws NoSuchFieldException,
      IllegalAccessException {
    Field field = findDeclaredField(target.getClass(), fieldname);
    field.setAccessible(true);
    field.set(target, value);
  }
  
  public static Field findDeclaredField(Class<?> inClass, String fieldname) throws NoSuchFieldException {
    while (!Object.class.equals(inClass)) {
      for (Field field : inClass.getDeclaredFields()) {
        if (field.getName().equalsIgnoreCase(fieldname)) {
          return field;
        }
      }
      inClass = inClass.getSuperclass();
    }
    throw new NoSuchFieldException();
  }
  
}
