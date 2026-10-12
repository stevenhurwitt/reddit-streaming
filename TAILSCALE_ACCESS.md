# Tailscale Access Setup

## Status: ✅ Ready to Use

Your Reddit Frontend is now accessible over Tailscale from any device on your network.

## Your Tailscale Details

- **Machine Name**: raspberrypi0
- **Tailscale IP**: 100.114.66.16
- **Frontend Port**: 3001
- **Status**: Active

## How to Access

### From any device on your Tailscale network:

```
http://100.114.66.16:3001
```

## Connected Devices

Currently active on your Tailscale network:
- **raspberrypi0** (linux) - 100.114.66.16 ← Your backend
- **iphone-12-mini** (iOS) - 100.98.148.65
- **raspberrypi3** (linux) - 100.100.141.40
- **stevens-macbook-air-3** (macOS) - 100.73.240.64 (active)

## What Was Changed

1. **nginx.conf**: Added IPv6 support and improved proxy headers
   - `listen [::]:3000` for IPv6 compatibility
   - Added `X-Forwarded-For` and `X-Forwarded-Proto` headers for proper request proxying

2. **Container Rebuilt**: Frontend container redeployed with updated config

## Verification

- ✅ Frontend HTTP 200 response on Tailscale IP
- ✅ API proxy configured and working
- ✅ All nginx worker processes active
- ✅ Port 3001 mapped correctly (0.0.0.0:3001 → 3000/tcp inside container)

## To Access from a New Device

1. Install Tailscale on the device: https://tailscale.com/download
2. Authenticate with your Tailscale account (same account as your main machine)
3. Access the frontend at `http://100.114.66.16:3001`

## Notes

- The frontend is only accessible to devices authenticated on your Tailscale network
- All traffic is encrypted end-to-end by Tailscale
- No port forwarding or firewall rules needed
- Works across different networks and ISPs
