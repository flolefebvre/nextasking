export type IsEqual<Actual, Expected> =
  (<Probe>() => Probe extends Actual ? 1 : 2) extends <Probe>() => Probe extends Expected ? 1 : 2 ? true : false;

export type Expect<Assertion extends true> = Assertion;
