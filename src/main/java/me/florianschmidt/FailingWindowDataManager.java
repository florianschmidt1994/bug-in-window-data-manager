package me.florianschmidt;

import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.WindowDataManager;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

public class FailingWindowDataManager
    extends BaseOperator
    implements Operator.CheckpointNotificationListener
{
  private WindowDataManager windowDataManager;

  private boolean ignoreTuplesInWindow;

  private transient Logger logger = LoggerFactory.getLogger(FailingWindowDataManager.class);

  @SuppressWarnings("unused") // kryo
  public FailingWindowDataManager()
  {
    super();
  }

  @Override
  public void beginWindow(long windowId)
  {
    if (windowId < windowDataManager.getLargestCompletedWindow()) {
      ignoreTuplesInWindow = true;
      return;
    }
    if (windowId == windowDataManager.getLargestCompletedWindow()) {
      if (alreadyEmitted(windowId)) {
        ignoreTuplesInWindow = true;
        return;
      }
    }
    ignoreTuplesInWindow = false;
    beginTransaction(windowId);
    super.beginWindow(windowId);
  }

  private void beginTransaction(long windowId)
  {
    String currentTxn = UUID.randomUUID().toString();
    try {
      this.windowDataManager.save(currentTxn, windowId);
    } catch (IOException e) {
      throw new RuntimeException("An exception occurred while saving transaction into window " +
          "data manager", e);
    }
  }

  @Override
  public void endWindow()
  {
    if (!ignoreTuplesInWindow) {
      logger.info("committing transaction");
    }
    super.endWindow();
  }

  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] event)
    {
      if (!ignoreTuplesInWindow) {
        emit(event);
      }
    }
  };

  private void emit(byte[] event)
  {
    logger.info("Emitting {}", event);
  }

  private boolean alreadyEmitted(long windowId)
  {
    String obtainedTransactionID;
    try {
      obtainedTransactionID = (String)this.windowDataManager.retrieve(windowId);
    } catch (IOException e) {
      throw new RuntimeException("An exception occurred while trying to receive the transaction " +
          "id for the current window from window data manager", e);
    }
    logger.info("Transaction {} found for window {}", obtainedTransactionID, windowId);
    return false;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    this.windowDataManager = new VerboseWindowDataManager();
    this.windowDataManager.setup(context);
  }

  @Override
  public void beforeCheckpoint(long l)
  {

  }

  @Override
  public void checkpointed(long l)
  {

  }

  @Override
  public void committed(long windowId)
  {
    try {
      windowDataManager.committed(windowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}