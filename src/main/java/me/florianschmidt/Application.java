/**
 * Put your copyright and license info here.
 */
package me.florianschmidt;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "FailingWindowDataManager")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build
    FailingWordGenerator generator = dag.addOperator(
        "FailingWordGenerator",
        FailingWordGenerator.class
    );

    FailingWindowDataManager dataManager = dag.addOperator(
        "failingWindowDataManager",
        FailingWindowDataManager.class
    );

    dag.addStream("randomData", generator.output, dataManager.input);
  }
}
