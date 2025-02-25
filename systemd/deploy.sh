#!/bin/bash

# Copy systemd files to appropriate location
sudo cp systemd/smstrack.* /etc/systemd/system/

# Reload systemd daemon
sudo systemctl daemon-reload

# Enable and start timer
sudo systemctl enable smstrack.timer
sudo systemctl start smstrack.timer

# Check status
systemctl status smstrack.timer