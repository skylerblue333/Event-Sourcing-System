import { DomainEvent } from './eventStore';

export interface AccountState {
  id: string;
  balance: number;
  version: number;
}

export class AccountAggregate {
  private state: AccountState;

  constructor(id: string) {
    this.state = { id, balance: 0, version: 0 };
  }

  apply(event: DomainEvent): void {
    switch (event.type) {
      case 'ACCOUNT_CREATED':
        this.state.balance = Number(event.data.initialBalance);
        break;
      case 'FUNDS_DEPOSITED':
        this.state.balance += Number(event.data.amount);
        break;
      case 'FUNDS_WITHDRAWN':
        this.state.balance -= Number(event.data.amount);
        break;
      default:
        throw new Error(`unknown account event: ${event.type}`);
    }

    this.state.version = event.sequence;
  }

  snapshot(): AccountState {
    return { ...this.state };
  }
}
