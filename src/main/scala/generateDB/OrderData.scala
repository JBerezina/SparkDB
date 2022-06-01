class OrderData(var customer_id:Int, var customer_name:String){
    var isAlive = true

    
    override def toString():String = {
        return this.customer_id + ": " + this.customer_name
    }
}
          
        

// payment_txn_success
// Payment Success or Failure (Y=Success. N=Failed)
// failure_reason
// Reason for payment failure
// Sample Data
  // var product_id:Int,
            // var product_name: String,
            // var product_category: String,
            // var  payment_type: String,
            // Payment Type (card, Internet Banking, UPI, Wallet)
            // var qty:Int,
            // var price: Float,
            // var datetim: Date,
            // var country: String,
            // var city: String,
            // var website: String,
            // var payment_id: Int,
            // var payment_success:String,
            // var failure_reason:String,