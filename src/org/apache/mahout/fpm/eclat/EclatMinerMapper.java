package ua.fim.eclat;

import org.apache.hadoop.io.Text;

import ua.fim.eclat.util.TreeStringReporter;
import ua.fim.eclat.util.SetReporter;

public class EclatMinerMapper extends EclatMinerMapperBase<Text> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new TreeStringReporter(context);
  }
}