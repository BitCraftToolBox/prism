import cron from 'node-cron';

import {main} from "./src/main";

let args: string[] = process.argv.slice(2);

if (args.length > 0 && cron.validate(args[0])) {
    const timeArg = args[0];
    const passArgs = args.slice(1);
    console.log("Scheduling task @", timeArg, "args:", passArgs);
    let task = cron.schedule(timeArg, () => {
        main(passArgs)
            .then(() => console.log("Finished scheduled run. Next run at", task.getNextRun()))
            .catch(console.error);
    });
} else {
    console.log("Running one-shot:", args);
    main(args)
        .then(() => process.exit(0))
        .catch((error) => {
            // @ts-ignore generated SDK disconnect errors include wasClean at runtime.
            if (error?.wasClean) {
                process.exit(0);
            }
            console.error("Error:", error);
            process.exit(1);
        });
}
