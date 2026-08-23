import { DomainEvent } from './eventStore';

export interface AccountState {
  id: string;
  balance: number;
  version: number;
}

function numericField(data: unknown, field: string): number {
  if (!data || typeof data !== 'object') {
    throw new Error(`event data must be an object containing ${field}`);
  }
  const value = (data as Record<string, unknown>)[field];
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(`${field} must be a non-negative finite number`);
  }
  return value;
}

export class AccountAggregate {
  private state: AccountState;

  constructor(id: string) {
    this.state = { id, balance: 0, version: 0 };
  }

  apply(event: DomainEvent): void {
    if (event.sequence !== this.state.version + 1) {
      throw new Error(`unexpected event sequence ${event.sequence}`);
    }

    switch (event.type) {
      case 'ACCOUNT_CREATED':
        if (this.state.version !== 0) throw new Error('account already created');
        this.state.balance = numericField(event.data, 'initialBalance');
        break;
      case 'FUNDS_DEPOSITED':
        this.state.balance += numericField(event.data, 'amount');
        break;
      case 'FUNDS_WITHDRAWN': {
        const amount = numericField(event.data, 'amount');
        if (amount > this.state.balance) throw new Error('insufficient balance');
        this.state.balance -= amount;
        break;
      }
      default:
        throw new Error(`unknown account event: ${event.type}`);
    }

    this.state.version = event.sequence;
  }

  snapshot(): AccountState {
    return { ...this.state };
  }
}
