    /* Method to get all the bytes that make up a string */
    private byte[] strToByteArray(string str)
    {
        System.Text.UTF8Encoding encoding = new System.Text.UTF8Encoding();
        return encoding.GetBytes(str);
    }

    /* Method to create a string as a hex representation of input bytes. */
    private string bytesToStr(byte[] bytes)
    {
        return BitConverter.ToString(bytes);
    }