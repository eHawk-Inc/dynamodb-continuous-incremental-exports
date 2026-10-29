import { isTableDisabled, normalizeTableName, parseDisabledTables } from '../lib/configuration';

describe('disabledTables', () => {
  it('parses a comma list and ignores blanks', () => {
    expect(parseDisabledTables(' RouteEntry, AuditRecord ,,')).toEqual(['RouteEntry', 'AuditRecord']);
    expect(parseDisabledTables(undefined)).toEqual([]);
    expect(parseDisabledTables('')).toEqual([]);
  });

  it('normalizes to the last name segment, lowercased', () => {
    expect(normalizeTableName('ehawk-mobilehub-1778525198-RouteEntry')).toBe('routeentry');
    expect(normalizeTableName('chat_Users')).toBe('users');
  });

  it('matches by logical name or full table name', () => {
    const list = ['RouteEntry', 'AuditRecord'];
    expect(isTableDisabled('ehawk-mobilehub-1778525198-RouteEntry', list)).toBe(true);
    expect(isTableDisabled('ehawk-mobilehub-1778525198-AuditRecord', list)).toBe(true);
    expect(isTableDisabled('ehawk-mobilehub-1778525198-Case', list)).toBe(false);
    expect(isTableDisabled('ehawk-mobilehub-1778525198-Case')).toBe(false);
  });
});
