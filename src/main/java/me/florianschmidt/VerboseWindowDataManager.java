package me.florianschmidt;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

import com.datatorrent.api.Context;

public class VerboseWindowDataManager extends FSWindowDataManager
{

  private transient Logger logger = LoggerFactory.getLogger(VerboseWindowDataManager.class);

  public VerboseWindowDataManager()
  {
    super();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    logger.info("--> setup");
    logger.info("    Activation window id {}", context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID));
    logger.info("    Application id {}", context.getValue(Context.DAGContext.APPLICATION_ID));
    logger.info("    Application name {}", context.getValue(Context.DAGContext.APPLICATION_NAME));
    logger.info("    Application path {}", context.getValue(Context.DAGContext.APPLICATION_PATH));
    logger.info("    State path relative to app path {}", super.isStatePathRelativeToAppPath());
    super.setup(context);
  }

  @Override
  public void save(Object object, long windowId) throws IOException
  {
    logger.info("--> save object {} for windowId {}", object, windowId);
    super.save(object, windowId);
  }

  @Override
  public Object retrieve(long windowId) throws IOException
  {
    logger.info("--> retrieve object for windowdId {}", windowId);
    Object o = super.retrieve(windowId);
    logger.info("<-- {}", o);
    return o;
  }

  @Override
  public Map<Integer, Object> retrieveAllPartitions(long windowId) throws IOException
  {
    logger.info("--> retrieve all partitions called");
    return super.retrieveAllPartitions(windowId);
  }

  @Override
  public void committed(long committedWindowId) throws IOException
  {
    logger.info("--> committed committedWindowId {}", committedWindowId);
    super.committed(committedWindowId);
  }

  @Override
  public long getLargestCompletedWindow()
  {
    logger.info("--> get largest completed window");
    long wId = super.getLargestCompletedWindow();
    logger.info("<-- {}", wId);
    return wId;
  }

  @Override
  public List<WindowDataManager> partition(int newCount, Set<Integer> removedOperatorIds)
  {
    logger.info("Partition called");
    return super.partition(newCount, removedOperatorIds);
  }

  @Override
  public void teardown()
  {
    logger.info("teardown");
    super.teardown();
  }

  @Override
  public Set<Integer> getDeletedOperators()
  {
    return super.getDeletedOperators();
  }

  @Override
  public String getStatePath()
  {
    return super.getStatePath();
  }

  @Override
  public void setStatePath(String statePath)
  {
    logger.info("Setting state path to {}", statePath);
    super.setStatePath(statePath);
  }

  @Override
  public boolean isStatePathRelativeToAppPath()
  {
    return super.isStatePathRelativeToAppPath();
  }

  @Override
  public void setStatePathRelativeToAppPath(boolean statePathRelativeToAppPath)
  {
    logger.info("setStatePathRelativeToAppPath to {}", statePathRelativeToAppPath);
    super.setStatePathRelativeToAppPath(statePathRelativeToAppPath);
  }
}
