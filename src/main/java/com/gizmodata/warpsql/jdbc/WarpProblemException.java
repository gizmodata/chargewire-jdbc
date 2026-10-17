package com.gizmodata.warpsql.jdbc;

/**
 * Carries a {@link WarpProblem} as the cause of the {@link java.sql.SQLException} the driver
 * raises, so callers can read the server's error code.
 */
public final class WarpProblemException extends Exception {
  private static final long serialVersionUID = 1L;

  private final transient WarpProblem problem;

  /**
   * Wraps a problem.
   *
   * @param problem the server's error
   */
  public WarpProblemException(WarpProblem problem) {
    super(problem.message());
    this.problem = problem;
  }

  /**
   * Returns the server's error.
   *
   * @return the problem
   */
  public WarpProblem getProblem() {
    return problem;
  }
}
