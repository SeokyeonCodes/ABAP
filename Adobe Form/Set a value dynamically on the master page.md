
## Set a value dynamically on the master page.
### Using javascript, can display different values on each page
#### In my case, I display a invoce No("123456") on the first page and with "_PKG(123456_PKG") on the second page.
1. Create two text fields; one of them is no binded and the other is binded.
2. Write coding below at the script on the no binded text field.
``` javascript
// Get the current value of the text field
  var currentValue = xfa.resolveNode("data.#pageSet.MainPage.Subform.(your binded TEXTFIELD name)").rawValue;

// Check if the current page is the first page
  if (xfa.layout.page(this) === 1) {
    // Set the value for the first page (keep the original value)
    this.rawValue = currentValue;
  } else {
    // Set the value for subsequent pages (Concatenate with "_PKG")
    this.rawValue = currentValue.concat("_PKG");
  }
```
3. Set the Presence to "Invisible" or "Visible (Print Only) on the binded text field not to display when printing.
