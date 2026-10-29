package com.gizmodata.chargewire.jdbc;

/**
 * Carries a {@link ChargewireProblem} as the cause of the {@link java.sql.SQLException} the driver
 * raises, so callers can read the server's error code.
 */
public final class ChargewireProblemException extends Exception {
  private static final long serialVersionUID = 1L;

  private final transient ChargewireProblem problem;

  /**
   * Wraps a problem.
   *
   * @param problem the server's error
   */
  public ChargewireProblemException(ChargewireProblem problem) {
    super(problem.message());
    this.problem = problem;
  }

  /**
   * Returns the server's error.
   *
   * @return the problem
   */
  public ChargewireProblem getProblem() {
    return problem;
  }
}
