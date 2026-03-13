# run_bulk_images.ps1
# Fill in your keys below, then right-click → Run with PowerShell

$env:REPLICATE_API_KEY   = "r8_PDaHKnl779I2xfkLz8N35Fs8YgGOouG37QGQC"
$env:SUPABASE_URL        = "https://ykuenmwfxecmmqichwit.supabase.co"
$env:SUPABASE_KEY        = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlrdWVubXdmeGVjbW1xaWNod2l0Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NzkxMDEzMCwiZXhwIjoyMDgzNDg2MTMwfQ.7OlFuiGHvno0xqD6QQ69Hw5Vpa_AErS7OpQ8L5TxJxI"

python bulk_generate_images.py
