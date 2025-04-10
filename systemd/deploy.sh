#!/bin/bash

# Copy systemd files to appropriate location
#sudo cp systemd/smstrack.* /etc/systemd/system/
sudo cp systemd/call_log_track.* /etc/systemd/system/

# Reload systemd daemon
sudo systemctl daemon-reload

# Enable and start timer
#sudo systemctl enable smstrack.timer
#sudo systemctl start smstrack.timer

sudo systemctl enable call_log_track.timer
sudo systemctl start call_log_track.timer

# Check status
# systemctl status smstrack.timer
