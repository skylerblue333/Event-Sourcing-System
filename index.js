const { EventEmitter } = require('events');

class EventStore extends EventEmitter {
  constructor() {
    super();
    this.events = [];
  }

  saveEvent(aggregateId, type, data) {
    const event = {
      aggregateId,
      type,
      data,
      timestamp: new Date().toISOString()
    };
    this.events.push(event);
    this.emit(type, event);
    console.log(`[EventStore] Saved: ${type}`);
  }

  getEventsForAggregate(aggregateId) {
    return this.events.filter(e => e.aggregateId === aggregateId);
  }
}

class AccountAggregate {
  constructor(id) {
    this.id = id;
    this.balance = 0;
  }

  apply(event) {
    switch (event.type) {
      case 'ACCOUNT_CREATED':
        this.balance = event.data.initialBalance;
        break;
      case 'FUNDS_DEPOSITED':
        this.balance += event.data.amount;
        break;
      case 'FUNDS_WITHDRAWN':
        this.balance -= event.data.amount;
        break;
    }
  }
}

// Demo
const store = new EventStore();

// Command Handler
function deposit(accountId, amount) {
  store.saveEvent(accountId, 'FUNDS_DEPOSITED', { amount });
}

// Projection / Query side
store.on('FUNDS_DEPOSITED', (event) => {
  console.log(`[Projection] Updating read model for account ${event.aggregateId}`);
});

// Run
store.saveEvent('acc-123', 'ACCOUNT_CREATED', { initialBalance: 100 });
deposit('acc-123', 50);

const acc = new AccountAggregate('acc-123');
store.getEventsForAggregate('acc-123').forEach(e => acc.apply(e));

console.log(`Final Balance: $${acc.balance}`);
