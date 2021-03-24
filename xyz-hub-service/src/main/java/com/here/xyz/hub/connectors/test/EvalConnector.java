package com.here.xyz.hub.connectors.test;

import com.amazonaws.util.IOUtils;
import com.google.common.base.Strings;
import com.here.xyz.Typed;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.ModifySpaceEvent.Operation;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EvalConnector extends AbstractConnectorHandler {

  private static final Logger logger = LogManager.getLogger();
  ScriptEngine engine;

  @Override
  protected void initialize(Event event) throws Exception {
    engine = new ScriptEngineManager().getEngineByName("js");
  }

  /**
   * Evaluate javascript code present in one of the following places,
   * picking the first non-empty value:
   * - event.params.code
   * - event.params.file
   * - event.connectorParams.code
   * - event.connectorParams.file
   *
   * @param event the incoming event
   * @return
   * @throws Exception
   */
  @Override
  protected Typed processEvent(Event event) throws Exception {
    if (event == null)
      return error("Nothing to eval. Event is null");

    if (event instanceof HealthCheckEvent || event instanceof ModifySpaceEvent && ((ModifySpaceEvent) event).getOperation() == Operation.DELETE)
      return new SuccessResponse().withStatus("OK");

    final String code = getCodeToEval(event);
    if (Strings.isNullOrEmpty(code))
      return error("Nothing to eval. Code is not available");

    SimpleScriptContext ctx = new SimpleScriptContext();
    Bindings engineScope = ctx.getBindings(ScriptContext.ENGINE_SCOPE);
    engineScope.put("streamId", streamId);
    engineScope.put("traceItem", traceItem);
    engineScope.put("event", event);

    try {
      engine.eval(code, ctx);
    } catch (ScriptException e) {
      return new ErrorResponse().withError(XyzError.ILLEGAL_ARGUMENT).withErrorMessage(e.getCause().getMessage());
    }

    return new SuccessResponse().withStatus("Eval OK");
  }

  private ErrorResponse error(String error) {
    return new ErrorResponse()
        .withStreamId(streamId)
        .withError(XyzError.EXCEPTION)
        .withErrorMessage(error);
  }

  private String getCodeToEval(Event event) {
    String code = null;
    try {
      if (event.getParams().containsKey("code"))
        code = (String) event.getParams().get("code");
      else if (event.getParams().containsKey("file"))
        code = IOUtils.toString(EvalConnector.class.getResourceAsStream((String) event.getParams().get("file")));
      else if (event.getConnectorParams().containsKey("code"))
        code = (String) event.getConnectorParams().get("code");
      else if (event.getConnectorParams().containsKey("file"))
        code = IOUtils.toString(EvalConnector.class.getResourceAsStream((String) event.getConnectorParams().get("file")));
    } catch (Exception e) {
      logger.error("Error retrieving the code from event.", e);
    }

    return code;
  }

  public static void main(String[] args) throws Exception {
    EvalConnector connector = new EvalConnector();
    connector.initialize(null);
    connector.processEvent(null);
  }
}
