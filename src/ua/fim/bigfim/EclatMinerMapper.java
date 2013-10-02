package ua.fim.bigfim;

import org.apache.hadoop.io.Text;

import ua.fim.bigfim.util.SetReporter;

public class EclatMinerMapper extends EclatMinerMapperBase<Text> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new SetReporter.HadoopTreeStringReporter(context);
  }
}