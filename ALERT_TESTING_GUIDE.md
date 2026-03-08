# Alert Testing Guide

## Issues Fixed

### 1. Air Quality Alerts (FIXED)
**Problem:** Alert triggered when AQI was WITHIN range instead of OUTSIDE
**Solution:** Changed comparison from `if min <= aqi <= max` to `if aqi < min OR aqi > max`

**Test Configuration:**
```json
{
  "alertType": "airQuality",
  "name": "Poor Air Quality Alert",
  "status": "active",
  "value": {
    "min": 0,
    "max": 100
  },
  "ownerId": "your_owner_id"
}
```
**Expected Behavior:** Alert triggers when AQI > 100 or AQI < 0 (outside safe range)
**Generated Values:** AQI between 100-350 (should mostly trigger)

---

### 2. Door Queue Alerts (FIXED)
**Problem:** Alert triggered when queue count was WITHIN range instead of OUTSIDE
**Solution:** Changed comparison from `if min <= count <= max` to `if count < min OR count > max`

**Test Configuration:**
```json
{
  "alertType": "doorQueue",
  "name": "Queue Alert",
  "status": "active",
  "value": {
    "min": 5,
    "max": 20
  },
  "ownerId": "your_owner_id"
}
```
**Expected Behavior:** Alert triggers when count < 5 or count > 20
**Generated Values:** Queue count between 5-50 (frequently > 20, should trigger)

---

### 3. Toilet Paper Alerts (FIXED)
**Problem:** No validation that 'min' was properly configured. Alerts triggered even when min=0
**Solution:** Added check that min_level must be configured (not None) before triggering

**Test Configuration:**
```json
{
  "alertType": "toiletPaper",
  "name": "Low Toilet Paper Alert",
  "status": "active",
  "value": {
    "min": 25
  },
  "ownerId": "your_owner_id"
}
```
**Expected Behavior:** Alert triggers ONLY when level < 25%
**Generated Values:** Level between 10-95% (~25% of time should trigger)
**Important:** If 'min' field is missing or not a number, alert is SKIPPED (won't trigger)

---

### 4. Soap Dispenser Alerts (FIXED)
**Problem:** No validation that 'min' was properly configured. Alerts triggered even when min=0
**Solution:** Added check that min_level must be configured (not None) before triggering

**Test Configuration:**
```json
{
  "alertType": "soapDispenser",
  "name": "Low Soap Alert",
  "status": "active",
  "value": {
    "min": 30
  },
  "ownerId": "your_owner_id"
}
```
**Expected Behavior:** Alert triggers ONLY when level < 30%
**Generated Values:** Level between 10-95% (~30-35% of time should trigger)
**Important:** If 'min' field is missing or not a number, alert is SKIPPED (won't trigger)

---

### 5. Handwash Alerts (FIXED)
**Problem:** No validation that 'min' was properly configured
**Solution:** Added check that min_level must be configured (not None) before triggering

**Test Configuration:**
```json
{
  "alertType": "handwash",
  "name": "Low Handwash Alert",
  "status": "active",
  "value": {
    "min": 25
  },
  "ownerId": "your_owner_id"
}
```
**Expected Behavior:** Alert triggers ONLY when level < 25%
**Generated Values:** Level between 10-95% (variable rate)

---

## Complete Alert Test Checklist

### For Toilet Paper Alert with min = 25:

1. **Insert test alerts in MongoDB:**
```javascript
db.alerts.insertOne({
  alertType: "toiletPaper",
  name: "TP Low Level",
  status: "active",
  value: { min: 25 },
  ownerId: "YOUR_OWNER_ID",
  severity: "warning"
})
```

2. **Check MongoDB logs** - Should see messages like:
```
[Alerts] Checking 1 alert(s) for toilet_paper
Toilet paper level: 45%, min threshold: 25
Alert skipped: level >= threshold
```

3. **When level < 25**, should see:
```
Toilet paper level: 15%, min threshold: 25
Alert condition met: 15 < 25
✅ TRIGGERED: TP Low Level
📧 Creating notification for alert...
```

4. **Check notifications collection** in MongoDB for created notifications:
```javascript
db.notifications.find({alertName: "TP Low Level"})
```

---

### For Air Quality Alert with min=0, max=100:

1. **Insert test alert:**
```javascript
db.alerts.insertOne({
  alertType: "airQuality",
  name: "Air Quality Alert",
  status: "active",
  value: { min: 0, max: 100 },
  ownerId: "YOUR_OWNER_ID"
})
```

2. **Check logs** - Should see alerts since generated AQI is 100-350:
```
AQI: 245, acceptable range: 0-100
Alert condition met: 245 > 100
✅ TRIGGERED: Air Quality Alert
```

---

## Debugging Commands

### Check all active alerts in MongoDB:
```javascript
db.alerts.find({status: "active"})
```

### Check recent notifications:
```javascript
db.notifications.find().sort({timestamp: -1}).limit(10)
```

### Check alert with missing 'min' field:
```javascript
db.alerts.findOne() // Look for 'value' field structure
```

---

## Summary of Changes

| Alert Type | Old Logic | New Logic | Key Fix |
|:---|:---|:---|:---|
| **Air Quality** | `if min<=aqi<=max` | `if aqi<min\|aqi>max` | Triggers when OUTSIDE range |
| **Door Queue** | `if min<=count<=max` | `if count<min\|count>max` | Triggers when OUTSIDE range |
| **Toilet Paper** | `min defaults to 0` | `min must be configured` | Won't trigger without explicit min |
| **Soap** | `min defaults to 0` | `min must be configured` | Won't trigger without explicit min |
| **Handwash** | `min defaults to 0` | `min must be configured` | Won't trigger without explicit min |

---

## Log Output Examples

### GOOD - Alert Configured and Triggered:
```
[Alerts] Checking 1 alert(s) for toilet_paper
Toilet paper level: 18%, min threshold: 25
Alert condition met: 18 < 25
✅ TRIGGERED: Low Toilet Paper Alert
📧 Notification created: ObjectId(...)
```

### GOOD - Alert Configured but Not Triggered:
```
[Alerts] Checking 1 alert(s) for toilet_paper
Toilet paper level: 72%, min threshold: 25
```
(No "Alert condition met" message = working as expected)

### PROBLEM - Min Not Configured:
```
[Alerts] Checking 1 alert(s) for toilet_paper
Toilet paper level: 45%, min threshold: None
Alert skipped: min_level not configured
```
Fix: Add "min" field to alert configuration in MongoDB

---

## Testing Steps

1. **Ensure MongoDB alerts have proper configuration:**
   - All alerts must have `status: "active"`
   - Toilet Paper/Soap/Handwash must have `value.min` set to a number
   - Air Quality must have `value.min` and `value.max` set

2. **Run the simulator:**
   ```bash
   python3 Restroomcode.py
   ```

3. **Monitor the logs** for alert trigger messages

4. **Check MongoDB notifications** collection to verify alerts were created

5. **Verify notification details** match the sensor data that triggered it
