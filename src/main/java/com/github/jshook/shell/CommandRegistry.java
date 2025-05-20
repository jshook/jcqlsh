    public void registerDefaultCommands() {
        registerCommand(new HelpCommand(this));
        registerCommand(new ExitCommand());
        registerCommand(new UseCommand(connectionManager));
        registerCommand(new DescribeCommand(connectionManager, formattingConfig));
        registerCommand(new TracingCommand(connectionManager));
        registerCommand(new ShowCommand(connectionManager, formattingConfig));
    }
