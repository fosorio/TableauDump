import java.lang.reflect.*;
import java.util.*;

public class ObjectGraphPrinter {
    private final Set<Integer> visited = new HashSet<>();
    private final Map<Integer, String> objectLabels = new HashMap<>();

    public void printGraph(Object root) {
        printGraph(root, "", new IdentityHashMap<>());
    }

    private void printGraph(Object obj, String indent, IdentityHashMap<Object, String> pathMap) {
        if (obj == null || obj instanceof String || obj.getClass().isPrimitive()) return;

        int id = System.identityHashCode(obj);
        if (visited.contains(id)) {
            System.out.println(indent + "(already visited) " + objectLabels.get(id));
            return;
        }

        String label = obj.getClass().getSimpleName() + "@" + id;
        objectLabels.put(id, label);
        visited.add(id);

        System.out.println(indent + label);

        Class<?> clazz = obj.getClass();
        List<Field> fields = new ArrayList<>();
        while (clazz != null) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }

        fields.sort(Comparator.comparing(Field::getName));

        for (Field field : fields) {
            field.setAccessible(true);
            if (Modifier.isStatic(field.getModifiers())) continue;

            try {
                Object child = field.get(obj);
                if (child == null || child instanceof String || field.getType().isPrimitive()) continue;

                int childId = System.identityHashCode(child);
                System.out.println(indent + "  ├─ " + field.getName() + " -> " + child.getClass().getSimpleName() + "@" + childId);
                printGraph(child, indent + "  │   ", pathMap);
            } catch (IllegalAccessException e) {
                System.out.println(indent + "  ├─ " + field.getName() + " -> (inaccessible)");
            }
        }
    }

    public Set<Integer> getVisitedIds() {
        return visited;
    }
}
