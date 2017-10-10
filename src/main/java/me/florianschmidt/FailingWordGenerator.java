package me.florianschmidt;

import com.datatorrent.api.Context;
import com.datatorrent.lib.testbench.RandomWordGenerator;

public class FailingWordGenerator extends RandomWordGenerator
{

  private int tupleCount = 0;

  private int failureEveryN = 20;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    super.setTuplesPerWindow(100);
  }

  @Override
  public void emitTuples()
  {
    super.emitTuples();
    tupleCount++;
    if (tupleCount % failureEveryN == 0) {
      throw new RuntimeException("Injecting failure at " + failureEveryN + " emitted tuple");
    }
  }

  public int getFailureEveryN()
  {
    return failureEveryN;
  }

  public void setFailureEveryN(int failureEveryN)
  {
    this.failureEveryN = failureEveryN;
  }
}
