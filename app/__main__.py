import argparse
import logging

from app import auth
from app.db import migrate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['api', 'worker', 'migrate', 'token', 'reconcile', 'replay'])
    parser.add_argument('--buyer', default='demo-customer')
    parser.add_argument('--event-id', type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    if args.command == 'migrate':
        migrate()
    elif args.command == 'token':
        print(auth.issue(args.buyer))
    elif args.command == 'api':
        import uvicorn
        uvicorn.run('app.api:app', host='0.0.0.0', port=8000, limit_concurrency=128, timeout_keep_alive=5)
    elif args.command == 'worker':
        from app.worker import run
        run()
    elif args.command == 'replay':
        if not args.event_id:
            parser.error('replay requires --event-id')
        from app.db import transaction
        with transaction() as conn:
            row = conn.execute('UPDATE outbox SET published_at=NULL,next_at=clock_timestamp(),failures=0,last_error=NULL WHERE id=%s RETURNING id', (args.event_id,)).fetchone()
            if not row:
                parser.error('Outbox event not found')
        print(f'Event {args.event_id} scheduled for idempotent replay')
    else:
        from app.service import reconcile
        print(reconcile())


if __name__ == '__main__':
    main()
