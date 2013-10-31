package org.apache.mahout.fpm.eclat;

import org.apache.hadoop.io.Text;
import org.apache.mahout.fpm.eclat.util.SetReporter;
import org.apache.mahout.fpm.eclat.util.TreeStringReporter;

public class EclatMinerMapper extends EclatMinerMapperBase<Text> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new TreeStringReporter(context);
  }
}