package whylogs.core.metrics.components;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TestIntegralComponent {

  @Test
  public void test_integral() {
    IntegralComponent component = new IntegralComponent(1);
    Assert.assertEquals((int) component.getValue(), 1);

    component = new IntegralComponent();
    Assert.assertEquals((int) component.getValue(), 0);

    Assert.assertEquals(component.getTypeId(), 0);
  }
}
