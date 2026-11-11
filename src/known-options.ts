type UnknownOptionMessage<OptionName> = OptionName extends string
  ? `"${OptionName}" is not an option of this call`
  : "not an option of this call";

type UnknownOptionsRefused<Given, Known> = Given extends object
  ? { [OptionName in Exclude<keyof Given, keyof Known>]: UnknownOptionMessage<OptionName> }
  : never;

type OptionNamesOf<Given> = { [OptionName in keyof Given]?: unknown };

type IsSameType<Left, Right> =
  (<Probe>() => Probe extends Left ? 1 : 2) extends <Probe>() => Probe extends Right ? 1 : 2 ? true : false;

type HasEveryMemberOfStringNumberOrBoolean<Given> = keyof string extends keyof Given
  ? true
  : keyof number extends keyof Given
    ? true
    : keyof boolean extends keyof Given
      ? true
      : false;

export type OnlyKnownOptions<Given, Known> = unknown extends Given
  ? Known
  : IsSameType<Given, Known> extends true
    ? Known
    : Given extends readonly unknown[]
      ? Known
      : HasEveryMemberOfStringNumberOrBoolean<Given> extends true
        ? Known
        : Known & UnknownOptionsRefused<Given, Known> & OptionNamesOf<Given>;
